"""
ETL Pipeline Builder — Streamlit Application

AI-assisted interface for building, visualizing, and executing ETL pipelines
using the microservices platform.

Run: streamlit run streamlit_app/app.py
"""

import glob
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import requests
import streamlit as st
import yaml

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from ai_agent.llm_provider import create_llm_provider  # noqa: E402
from ai_agent.pipeline_agent import PipelineAgent, validate_pipeline  # noqa: E402
from ai_agent.pipeline_compiler import PipelineCompiler  # noqa: E402

# ── Page Config ──
st.set_page_config(
    page_title="ETL Pipeline Builder",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Load configs ──
SERVICES_CONFIG_PATH = os.path.join(PROJECT_ROOT, "preparator", "services_config.json")
SERVICE_REGISTRY_PATH = os.path.join(PROJECT_ROOT, "schemas", "service_registry.json")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
_DEFAULT_AIRFLOW_USERNAME = "admin"
_DEFAULT_AIRFLOW_ADMIN_SHA256 = "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918"


@st.cache_data
def load_service_registry():
    with open(SERVICE_REGISTRY_PATH) as f:
        return json.load(f)


@st.cache_data
def load_services_config():
    with open(SERVICES_CONFIG_PATH) as f:
        return json.load(f)


def init_session_state():
    """Initialize session state variables."""
    defaults = {
        "messages": [],
        "pipeline_yaml": None,
        "pipeline_def": None,
        "execution_result": None,
        "execution_log": [],
        "step_previews": {},  # step_id -> Arrow IPC bytes for data preview
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ── Sidebar: Chat Interface ──
def render_sidebar():
    with st.sidebar:
        st.title("💬 Pipeline Chat")
        st.caption("Describe the ETL pipeline you need in natural language.")

        # LLM Provider selector
        provider = st.selectbox(
            "LLM Provider",
            ["openai", "openrouter", "local"],
            help=(
                "**OpenAI**: requires OPENAI_API_KEY (paid).  \n"
                "**OpenRouter**: gateway to 200+ models, including free ones — "
                "get a key at https://openrouter.ai/keys.  \n"
                "**Local**: uses the HuggingFace text-completion-llm-service (requires model download)."
            ),
        )

        if provider == "openai":
            api_key = st.text_input("OpenAI API Key", type="password", value=os.getenv("OPENAI_API_KEY", ""))
            if api_key:
                os.environ["OPENAI_API_KEY"] = api_key

        elif provider == "openrouter":
            or_key = st.text_input(
                "OpenRouter API Key", type="password",
                value=os.getenv("OPENROUTER_API_KEY", ""),
                help="Free key at https://openrouter.ai/keys",
            )
            if or_key:
                os.environ["OPENROUTER_API_KEY"] = or_key

            or_model = st.selectbox(
                "Model",
                [
                    "meta-llama/llama-3.1-8b-instruct:free",
                    "google/gemma-2-9b-it:free",
                    "mistralai/mistral-7b-instruct:free",
                    "qwen/qwen-2.5-7b-instruct:free",
                    "meta-llama/llama-3.3-70b-instruct",
                    "anthropic/claude-3.5-sonnet",
                    "openai/gpt-4o-mini",
                ],
                help="Models ending in `:free` require no credits.",
            )
            os.environ["OPENROUTER_MODEL"] = or_model

        st.divider()

        # Chat messages
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        # Chat input
        if user_input := st.chat_input("Describe your pipeline..."):
            st.session_state.messages.append({"role": "user", "content": user_input})

            with st.chat_message("user"):
                st.markdown(user_input)

            with st.chat_message("assistant"):
                with st.spinner("Generating pipeline..."):
                    try:
                        llm = create_llm_provider(provider=provider)
                        agent = PipelineAgent(llm)
                        pipeline_def = agent.generate_pipeline(user_input)
                        pipeline_yaml = yaml.dump(pipeline_def, default_flow_style=False, sort_keys=False)

                        st.session_state.pipeline_yaml = pipeline_yaml
                        st.session_state.pipeline_def = pipeline_def

                        explanation = agent.explain_pipeline(pipeline_def)
                        response = f"Pipeline generated successfully!\n\n{explanation}"

                        st.markdown(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                    except Exception as e:
                        error_msg = f"Error generating pipeline: {str(e)}"
                        st.error(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})


# ── Main: Pipeline Editor & Execution ──
def render_pipeline_editor():
    st.header("📝 Pipeline Definition")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("YAML Editor")
        default_yaml = st.session_state.pipeline_yaml or _default_pipeline_yaml()

        edited_yaml = st.text_area(
            "Edit pipeline YAML",
            value=default_yaml,
            height=400,
            label_visibility="collapsed",
        )

        if st.button("✅ Validate", use_container_width=True):
            try:
                pipeline_def = yaml.safe_load(edited_yaml)
                # Use the standalone validate_pipeline — no LLM needed
                errors, warnings = validate_pipeline(pipeline_def, load_service_registry())

                if errors:
                    for err in errors:
                        st.error(f"❌ {err}")
                else:
                    st.success("✅ Pipeline is valid!")
                    st.session_state.pipeline_def = pipeline_def
                    st.session_state.pipeline_yaml = edited_yaml
                for w in warnings:
                    st.warning(f"⚠️ {w}")
            except yaml.YAMLError as e:
                st.error(f"Invalid YAML syntax: {e}")

    with col2:
        st.subheader("Pipeline Preview")
        if st.session_state.pipeline_def:
            pipeline = st.session_state.pipeline_def["pipeline"]
            st.info(f"**{pipeline['name']}** — {len(pipeline['steps'])} steps")
            registry = load_service_registry()  # load once outside the loop

            for i, step in enumerate(pipeline["steps"]):
                svc = step["service"]
                svc_info = registry["services"].get(svc, {})
                svc_name = svc_info.get("name", svc)
                svc_type = svc_info.get("type", "unknown")

                icon = {"extract": "📥", "transform": "⚙️", "load": "📤"}.get(svc_type, "❓")
                params = step.get("params", {})
                params_str = ", ".join(f"`{k}={v}`" for k, v in params.items()) if params else "_default_"

                st.markdown(f"{icon} **{i+1}. {svc_name}** (`{step['id']}`)")
                st.caption(params_str)
                if i < len(pipeline["steps"]) - 1:
                    st.markdown("↓")
        else:
            st.info("Use the chat to generate a pipeline, or edit the YAML directly.")


def _probe_http(url: str, timeout: int = 3):
    start = time.time()
    try:
        resp = requests.get(url, timeout=timeout)
        elapsed_ms = round((time.time() - start) * 1000)
        return resp.status_code, elapsed_ms, None, resp
    except requests.Timeout:
        return None, None, "timeout", None
    except requests.ConnectionError:
        return None, None, "unreachable", None
    except Exception as exc:
        return None, None, str(exc), None


def _airflow_quick_trigger(dag_id: str, conf: dict | None = None):
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": conf or {}}
    try:
        resp = requests.post(
            url,
            json=payload,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10,
        )
    except Exception as exc:
        return False, f"Request failed: {exc}"

    if resp.status_code in (200, 201):
        run_id = resp.json().get("dag_run_id", "(unknown)")
        return True, f"Triggered `{dag_id}` with run_id `{run_id}`"

    try:
        details = resp.json()
    except Exception:
        details = resp.text[:300]
    return False, f"Airflow API error (HTTP {resp.status_code}): {details}"


def _render_platform_readiness():
    st.subheader("🩺 Platform Readiness")

    current_pwd_hash = hashlib.sha256(AIRFLOW_PASSWORD.encode("utf-8")).hexdigest()
    if AIRFLOW_USERNAME == _DEFAULT_AIRFLOW_USERNAME and current_pwd_hash == _DEFAULT_AIRFLOW_ADMIN_SHA256:
        st.warning(
            "Airflow is using default admin credentials. Rotate credentials before sharing this environment."
        )

    endpoints = [
        ("Airflow UI", f"{AIRFLOW_BASE_URL}/health"),
        ("Streamlit", "http://localhost:8501"),
        ("Prometheus", "http://localhost:9090/-/healthy"),
        ("Grafana", "http://localhost:3000/api/health"),
    ]

    rows = []
    for name, url in endpoints:
        code, elapsed, err, _ = _probe_http(url)
        if code == 200:
            status = "🟢 Ready"
        elif code is not None:
            status = f"🟡 HTTP {code}"
        elif err == "timeout":
            status = "🟠 Timeout"
        else:
            status = "🔴 Unreachable"
        rows.append({
            "Component": name,
            "Status": status,
            "Latency": f"{elapsed}ms" if elapsed is not None else "-",
            "URL": url,
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    airflow_health_url = f"{AIRFLOW_BASE_URL}/health"
    code, _, err, resp = _probe_http(airflow_health_url)
    if code == 200 and resp is not None:
        try:
            health = resp.json()
            scheduler_ok = health.get("scheduler", {}).get("status") == "healthy"
            if scheduler_ok:
                st.success("Airflow scheduler heartbeat is healthy.")
            else:
                st.warning("Airflow is reachable but scheduler is not fully healthy yet.")
        except Exception:
            st.info("Airflow health endpoint returned a non-JSON response.")
    elif err:
        st.warning("Airflow health check is not ready yet.")


def _render_airflow_quick_actions():
    st.subheader("⚡ Quick Airflow Triggers")
    dag_options = ["hr_analytics_pipeline", "ecommerce_pipeline", "weather_api_pipeline"]
    dag_id = st.selectbox("Choose DAG", dag_options, key="quick_trigger_dag")

    conf_text = st.text_area(
        "Run conf (JSON, optional)",
        value="{}",
        height=90,
        key="quick_trigger_conf",
        help="Optional DAG run config passed to Airflow API.",
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("🚀 Trigger in Airflow", use_container_width=True):
            try:
                conf = json.loads(conf_text.strip()) if conf_text.strip() else {}
                if not isinstance(conf, dict):
                    st.error("Run conf must be a JSON object.")
                else:
                    ok, msg = _airflow_quick_trigger(dag_id, conf)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
            except json.JSONDecodeError as exc:
                st.error(f"Invalid JSON in run conf: {exc}")
    with c2:
        st.link_button("🌐 Open Airflow", AIRFLOW_BASE_URL, use_container_width=True)


def render_execution_panel():
    st.header("🚀 Execution")

    _render_platform_readiness()
    _render_airflow_quick_actions()
    st.divider()

    col1, col2 = st.columns([1, 2])

    with col1:
        execute_disabled = st.session_state.pipeline_def is None
        if st.button(
            "▶️ Execute Pipeline", type="primary",
            use_container_width=True, disabled=execute_disabled,
        ):
            if st.session_state.pipeline_def:
                _execute_pipeline(st.session_state.pipeline_def)

    with col2:
        if st.session_state.execution_result:
            result = st.session_state.execution_result
            status_color = "🟢" if result.status == "completed" else "🔴"
            st.markdown(f"{status_color} **Status:** {result.status} | **Duration:** {result.total_duration_sec:.2f}s")

    # Step results table
    if st.session_state.execution_result:
        result = st.session_state.execution_result

        success_steps = sum(1 for s in result.steps if s.status == "success")
        total_steps = len(result.steps)
        total_kb = sum((s.data_size_bytes or 0) for s in result.steps) / 1024
        active_sec = sum(float(s.duration_sec or 0) for s in result.steps)
        overhead_sec = max(float(result.total_duration_sec or 0) - active_sec, 0.0)
        overhead_pct = (overhead_sec / result.total_duration_sec * 100.0) if result.total_duration_sec else 0.0
        slowest = max(result.steps, key=lambda s: s.duration_sec)
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Successful steps", f"{success_steps}/{total_steps}")
        m2.metric("Total processed", f"{total_kb:.1f} KB")
        m3.metric("Slowest step", slowest.step_id)
        m4.metric("Slowest duration", f"{slowest.duration_sec:.2f}s")
        m5.metric("Orchestration overhead", f"{overhead_pct:.1f}%")

        steps_data = []
        for s in result.steps:
            steps_data.append({
                "Step": s.step_id,
                "Service": s.service,
                "Status": "✅" if s.status == "success" else "❌",
                "Duration (s)": round(s.duration_sec, 3),
                "Data Size (KB)": round(s.data_size_bytes / 1024, 1) if s.data_size_bytes else 0,
                "Error": s.error_message or "",
            })
        st.dataframe(pd.DataFrame(steps_data), use_container_width=True, hide_index=True)

        # ── Data Preview & Download ──
        st.subheader("📊 Data Preview")
        if st.session_state.step_previews:
            preview_step = st.selectbox(
                "Select step to preview",
                list(st.session_state.step_previews.keys()),
                index=len(st.session_state.step_previews) - 1,
            )
            if preview_step and preview_step in st.session_state.step_previews:
                preview_data = st.session_state.step_previews[preview_step]
                if isinstance(preview_data, bytes) and len(preview_data) > 0:
                    try:
                        reader = pa_ipc.open_stream(preview_data)
                        preview_table = reader.read_all()
                        preview_df = preview_table.to_pandas()

                        col_info, col_preview = st.columns([1, 2])
                        with col_info:
                            st.metric("Rows", preview_df.shape[0])
                            st.metric("Columns", preview_df.shape[1])
                            dtypes_str = ', '.join(
                                f'{c}({d})'
                                for c, d in zip(
                                    preview_df.columns[:5],
                                    preview_df.dtypes[:5],
                                )
                            )
                            st.caption(f"dtypes: {dtypes_str}")
                        with col_preview:
                            max_rows = st.slider("Preview rows", 5, 100, 20, key=f"preview_{preview_step}")
                            st.dataframe(preview_df.head(max_rows), use_container_width=True, hide_index=True)

                        # Download buttons
                        dl_col1, dl_col2, dl_col3 = st.columns(3)
                        with dl_col1:
                            csv_data = preview_df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                "⬇️ CSV", csv_data,
                                f"{preview_step}.csv", "text/csv",
                                use_container_width=True,
                            )
                        with dl_col2:
                            json_data = preview_df.to_json(orient="records").encode("utf-8")
                            st.download_button(
                                "⬇️ JSON", json_data,
                                f"{preview_step}.json",
                                "application/json",
                                use_container_width=True,
                            )
                        with dl_col3:
                            st.download_button(
                                "⬇️ Arrow IPC", preview_data,
                                f"{preview_step}.arrow",
                                "application/octet-stream",
                                use_container_width=True,
                            )
                    except Exception as e:
                        st.warning(f"Cannot preview data for step '{preview_step}': {e}")
                else:
                    st.info(f"Step '{preview_step}' output is not Arrow IPC (e.g. load_data returns JSON status).")
        else:
            st.info("Execute a pipeline to see data previews.")

    # Execution log
    if st.session_state.execution_log:
        with st.expander("📋 Execution Log"):
            for entry in st.session_state.execution_log:
                st.text(entry)


def _execute_pipeline(pipeline_def: dict):
    """Execute pipeline with progress tracking and data capture."""
    st.session_state.execution_log = []
    st.session_state.step_previews = {}
    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(step_id, status, pct):
        progress_bar.progress(int(pct))
        status_text.text(f"Step '{step_id}': {status}")
        st.session_state.execution_log.append(f"[{time.strftime('%H:%M:%S')}] {step_id}: {status}")

    try:
        config = load_services_config()
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "preparator"))
        from preparator_v4 import Preparator

        with Preparator(config) as prep:
            compiler = PipelineCompiler(prep)
            result = compiler.execute(pipeline_def, progress_callback=progress_callback)
            st.session_state.execution_result = result

            # Capture step outputs for preview
            for step_id, output in compiler.last_step_outputs.items():
                if isinstance(output, bytes) and len(output) > 0:
                    st.session_state.step_previews[step_id] = output

        progress_bar.progress(100)
        if result.status == "completed":
            st.success(f"Pipeline completed in {result.total_duration_sec:.2f}s")
        else:
            st.error(f"Pipeline failed: {result.steps[-1].error_message}")

    except Exception as e:
        st.error(f"Execution error: {str(e)}")
        st.session_state.execution_log.append(f"[{time.strftime('%H:%M:%S')}] FATAL: {str(e)}")


def render_service_catalog():
    """Show available services, their parameters, and live health status."""
    st.header("📚 Service Catalog")

    # ── Live Health Dashboard ──
    st.subheader("🏥 Service Health")
    registry = load_service_registry()
    config = load_services_config()

    if st.button("🔄 Refresh Health", use_container_width=False):
        st.cache_data.clear()

    health_data = []
    for key, svc in registry["services"].items():
        svc_type = svc.get("type", "unknown")
        icon = {"extract": "📥", "transform": "⚙️", "load": "📤"}.get(svc_type, "❓")
        url = config.get(key, "")
        # Derive health URL from service URL
        if url:
            base_url = url.rsplit("/", 1)[0]
            health_url = f"{base_url}/health"
        else:
            health_url = ""

        status = "⚪ Unknown"
        latency = "-"
        if health_url:
            try:
                start = time.time()
                resp = requests.get(health_url, timeout=3)
                elapsed = round((time.time() - start) * 1000)
                if resp.status_code == 200:
                    status = "🟢 Healthy"
                    latency = f"{elapsed}ms"
                else:
                    status = f"🟡 HTTP {resp.status_code}"
                    latency = f"{elapsed}ms"
            except requests.ConnectionError:
                status = "🔴 Unreachable"
            except requests.Timeout:
                status = "🟠 Timeout"
            except Exception:
                status = "🔴 Error"

        health_data.append({
            "": icon,
            "Service": svc["name"],
            "Key": key,
            "Type": svc_type,
            "Status": status,
            "Latency": latency,
        })

    st.dataframe(pd.DataFrame(health_data), use_container_width=True, hide_index=True)

    st.divider()

    # ── Service Details ──
    st.subheader("📖 Service Details")

    for key, svc in registry["services"].items():
        svc_type = svc.get("type", "unknown")
        icon = {"extract": "📥", "transform": "⚙️", "load": "📤"}.get(svc_type, "❓")

        with st.expander(f"{icon} {svc['name']} (`{key}`)"):
            st.markdown(svc["description"])
            st.caption(f"Type: {svc_type} | Input: {svc['input_format']} | Output: {svc['output_format']}")

            if svc.get("params"):
                params_data = []
                for pname, pinfo in svc["params"].items():
                    params_data.append({
                        "Parameter": pname,
                        "Type": pinfo["type"],
                        "Required": "✅" if pinfo.get("required") else "",
                        "Default": str(pinfo.get("default", "")),
                        "Description": pinfo["description"],
                    })
                st.dataframe(pd.DataFrame(params_data), use_container_width=True, hide_index=True)


# ── Dataset Explorer ──

DATA_ROOT = os.getenv("ETL_DATA_ROOT", "/app/data")

# Folders that contain raw demo assets, not pipeline outputs
_IGNORE_DATASETS = {"demo"}


def _scan_datasets():
    """Discover datasets on the shared volume. Returns sorted list of (name, path) tuples."""
    if not os.path.isdir(DATA_ROOT):
        return []
    entries = []
    for name in sorted(os.listdir(DATA_ROOT)):
        full = os.path.join(DATA_ROOT, name)
        if os.path.isdir(full) and name not in _IGNORE_DATASETS:
            entries.append((name, full))
    return entries


def _load_metadata_files(dataset_path):
    """Load all metadata JSONs for a dataset, newest first."""
    meta_dir = os.path.join(dataset_path, "metadata")
    if not os.path.isdir(meta_dir):
        return []
    items = []
    for fp in sorted(glob.glob(os.path.join(meta_dir, "metadata_*.json")), reverse=True):
        try:
            with open(fp) as f:
                items.append(json.load(f))
        except (OSError, json.JSONDecodeError):
            continue
    return items


def _list_output_files(dataset_path):
    """List processed output files with size and mtime."""
    proc_dir = os.path.join(dataset_path, "processed")
    if not os.path.isdir(proc_dir):
        return []
    files = []
    for name in sorted(os.listdir(proc_dir), reverse=True):
        fp = os.path.join(proc_dir, name)
        if os.path.isfile(fp):
            stat = os.stat(fp)
            files.append({
                "name": name,
                "path": fp,
                "size_bytes": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            })
    return files


def _read_output_preview(filepath, max_rows=100):
    """Read a processed output file into a DataFrame for preview."""
    ext = Path(filepath).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(filepath, nrows=max_rows)
    elif ext == ".parquet":
        table = pq.read_table(filepath)
        return table.to_pandas().head(max_rows)
    elif ext == ".json":
        return pd.read_json(filepath).head(max_rows)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(filepath, nrows=max_rows)
    return None


def _parse_ts(ts_value):
    if not ts_value:
        return None
    try:
        normalized = str(ts_value).replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _build_run_timeline(steps):
    timeline = []
    prev_end = None
    for idx, s in enumerate(steps, start=1):
        end_ts = _parse_ts(s.get("timestamp"))
        duration_sec = float(s.get("duration_sec", 0) or 0)
        start_ts = end_ts - timedelta(seconds=duration_sec) if end_ts is not None else None
        queue_gap_sec = 0.0
        if prev_end is not None and start_ts is not None:
            queue_gap_sec = max((start_ts - prev_end).total_seconds(), 0.0)
        if end_ts is not None:
            prev_end = end_ts

        timeline.append({
            "#": idx,
            "Step": s.get("service_name", "?"),
            "Active (s)": round(duration_sec, 3),
            "Queue/Gap (s)": round(queue_gap_sec, 3),
            "Started": start_ts.strftime("%H:%M:%S") if start_ts else "-",
            "Finished": end_ts.strftime("%H:%M:%S") if end_ts else "-",
        })
    return timeline


def _derive_business_kpis(df):
    kpis = []
    cols = set(df.columns)

    if "TotalAmount" in cols:
        amount = pd.to_numeric(df["TotalAmount"], errors="coerce")
        revenue = float(amount.fillna(0).sum())
        orders = len(df)
        aov = (revenue / orders) if orders else 0.0
        kpis.extend([
            ("Revenue", f"{revenue:,.2f}"),
            ("Orders", f"{orders:,}"),
            ("Avg Order Value", f"{aov:,.2f}"),
        ])

    if "Attrition" in cols:
        attr = df["Attrition"].astype(str).str.strip().str.lower()
        positive = attr.isin(["yes", "true", "1", "y"]).sum()
        total = len(attr)
        rate = (positive / total * 100.0) if total else 0.0
        kpis.extend([
            ("Employees", f"{total:,}"),
            ("Attrition Cases", f"{int(positive):,}"),
            ("Attrition Rate", f"{rate:.2f}%"),
        ])

    if "temperature" in {c.lower() for c in cols}:
        temp_col = next(c for c in df.columns if c.lower() == "temperature")
        temp = pd.to_numeric(df[temp_col], errors="coerce")
        if temp.notna().any():
            kpis.extend([
                ("Avg Temperature", f"{float(temp.mean()):.1f}"),
                ("Min Temperature", f"{float(temp.min()):.1f}"),
                ("Max Temperature", f"{float(temp.max()):.1f}"),
            ])

    if not kpis:
        total_cells = max(df.shape[0] * df.shape[1], 1)
        null_ratio = float(df.isna().sum().sum()) / total_cells
        kpis.extend([
            ("Rows", f"{df.shape[0]:,}"),
            ("Columns", f"{df.shape[1]:,}"),
            ("Completeness", f"{(1.0 - null_ratio) * 100:.2f}%"),
        ])

    return kpis


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_run_summary(cid, steps):
    if not steps:
        return {
            "cid": cid,
            "duration_s": 0.0,
            "rows": None,
            "outliers": 0,
            "success": False,
            "end_ts": None,
        }

    timeline_rows = _build_run_timeline(steps)
    active_total = sum(r["Active (s)"] for r in timeline_rows)
    gap_total = sum(r["Queue/Gap (s)"] for r in timeline_rows)
    duration_s = active_total + gap_total

    end_ts_candidates = [_parse_ts(s.get("timestamp")) for s in steps]
    end_ts_candidates = [ts for ts in end_ts_candidates if ts is not None]
    end_ts = max(end_ts_candidates) if end_ts_candidates else None

    last_step = steps[-1]
    rows = last_step.get("rows_out", last_step.get("rows_in"))
    if rows is not None:
        rows = _to_int(rows, default=0)

    outliers = 0
    for s in steps:
        if "removed_outliers" in s:
            outliers += _to_int(s.get("removed_outliers"), default=0)

    success = any(
        ("output_file" in s)
        or ("load" in str(s.get("service_name", "")).lower())
        for s in steps
    )

    return {
        "cid": cid,
        "duration_s": round(duration_s, 3),
        "rows": rows,
        "outliers": outliers,
        "success": success,
        "end_ts": end_ts,
    }


def _delta_text(current, baseline, suffix=""):
    if current is None or baseline is None:
        return "n/a"
    diff = _to_float(current) - _to_float(baseline)
    return f"{diff:+.2f}{suffix}"


def _group_runs(metadata_items):
    """Group metadata entries by correlation_id → pipeline runs."""
    runs = defaultdict(list)
    for m in metadata_items:
        cid = m.get("correlation_id", "unknown")
        runs[cid].append(m)
    # Sort steps within each run by timestamp
    for cid in runs:
        runs[cid].sort(key=lambda m: m.get("timestamp", ""))
    return dict(runs)


def _format_bytes(n):
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def render_dataset_explorer():
    """Browse datasets, view pipeline run history, preview & download output files."""
    st.header("📂 Dataset Explorer")

    datasets = _scan_datasets()
    if not datasets:
        st.info("No datasets found. Run a pipeline first, or load demo data with `make demo-data`.")
        return

    # Dataset selector
    ds_names = [name for name, _ in datasets]
    selected = st.selectbox("Select dataset", ds_names, key="ds_explorer_select")
    ds_path = dict(datasets)[selected]

    # ── Output Files ──
    output_files = _list_output_files(ds_path)
    metadata_items = _load_metadata_files(ds_path)

    # Business-friendly summary cards for the selected dataset.
    latest_rows = metadata_items[0].get("rows_out", metadata_items[0].get("rows_in", "-")) if metadata_items else "-"
    total_duration = sum(float(m.get("duration_sec", 0) or 0) for m in metadata_items)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Metadata events", len(metadata_items))
    c2.metric("Output files", len(output_files))
    c3.metric("Latest rows", latest_rows)
    c4.metric("Accumulated duration", f"{total_duration:.2f}s")

    if output_files:
        latest_path = output_files[0]["path"]
        latest_df = _read_output_preview(latest_path, max_rows=5000)
        if latest_df is not None and not latest_df.empty:
            st.caption("Business KPI snapshot from latest output")
            kpis = _derive_business_kpis(latest_df)
            cols = st.columns(min(len(kpis), 4))
            for idx, (label, value) in enumerate(kpis[:4]):
                cols[idx].metric(label, value)

    tab_out, tab_runs, tab_meta = st.tabs(["📄 Output Files", "🔄 Pipeline Runs", "🔍 Raw Metadata"])

    # ─── Tab 1: Output files with preview & download ───
    with tab_out:
        if not output_files:
            st.info(f"No output files yet for **{selected}**. The `load_data` step writes files here.")
        else:
            st.caption(f"{len(output_files)} file(s) in `{selected}/processed/`")

            for i, finfo in enumerate(output_files):
                ext = Path(finfo['name']).suffix.lstrip('.')
                icon = {"csv": "📊", "parquet": "🗂️", "json": "📋", "xlsx": "📗", "xls": "📗"}.get(ext, "📄")
                col_info, col_dl = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"{icon} **{finfo['name']}**  \n"
                        f"`{_format_bytes(finfo['size_bytes'])}` · "
                        f"{finfo['mtime'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                with col_dl:
                    with open(finfo["path"], "rb") as fp:
                        mime = {
                            "csv": "text/csv", "json": "application/json",
                            "parquet": "application/octet-stream",
                            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        }.get(ext, "application/octet-stream")
                        st.download_button(
                            "⬇️ Download", fp.read(), finfo["name"], mime,
                            key=f"dl_{selected}_{i}", use_container_width=True,
                        )

                # Preview
                with st.expander(f"Preview {finfo['name']}", expanded=(i == 0)):
                    try:
                        preview_df = _read_output_preview(finfo["path"])
                        if preview_df is not None:
                            c1, c2, c3 = st.columns(3)
                            c1.metric("Rows", preview_df.shape[0])
                            c2.metric("Columns", preview_df.shape[1])
                            c3.metric("Format", ext.upper())
                            st.dataframe(preview_df, use_container_width=True, hide_index=True)
                        else:
                            st.warning(f"Preview not supported for .{ext} files.")
                    except Exception as e:
                        st.warning(f"Cannot preview: {e}")

                if i < len(output_files) - 1:
                    st.divider()

    # ─── Tab 2: Pipeline runs grouped by correlation ID ───
    with tab_runs:
        if not metadata_items:
            st.info(f"No pipeline run history for **{selected}**.")
        else:
            runs = _group_runs(metadata_items)
            st.caption(f"{len(runs)} pipeline run(s) detected")

            run_summaries = []
            for cid, steps in runs.items():
                summary = _build_run_summary(cid, steps)
                run_summaries.append(summary)
            run_summaries.sort(key=lambda r: r["end_ts"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

            if run_summaries:
                current = run_summaries[0]
                baseline = next((r for r in run_summaries[1:] if r["success"]), None)
                st.subheader("📈 Run Comparison")
                if baseline is None:
                    st.info("Need at least two successful runs to compare current vs baseline.")
                else:
                    st.caption(
                        f"Current `{current['cid'][:8]}` vs previous successful `{baseline['cid'][:8]}`"
                    )
                    c1, c2, c3 = st.columns(3)
                    c1.metric(
                        "Duration",
                        f"{current['duration_s']:.2f}s",
                        delta=_delta_text(current["duration_s"], baseline["duration_s"], "s"),
                        delta_color="inverse",
                    )
                    c2.metric(
                        "Final rows",
                        "-" if current["rows"] is None else f"{current['rows']:,}",
                        delta=_delta_text(current["rows"], baseline["rows"]),
                    )
                    c3.metric(
                        "Outliers removed",
                        f"{current['outliers']}",
                        delta=_delta_text(current["outliers"], baseline["outliers"]),
                        delta_color="inverse",
                    )

            for run_idx, (cid, steps) in enumerate(runs.items()):
                first_ts = steps[0].get("timestamp", "?")
                n_steps = len(steps)
                total_dur = sum(s.get("duration_sec", 0) for s in steps)

                header = f"Run {run_idx + 1} — {n_steps} steps · {total_dur:.2f}s · {first_ts}"
                with st.expander(header, expanded=(run_idx == 0)):
                    st.caption(f"Correlation ID: `{cid}`")

                    timeline_rows = _build_run_timeline(steps)
                    active_total = sum(r["Active (s)"] for r in timeline_rows)
                    gap_total = sum(r["Queue/Gap (s)"] for r in timeline_rows)
                    wall_est = active_total + gap_total
                    t1, t2, t3 = st.columns(3)
                    t1.metric("Active processing", f"{active_total:.2f}s")
                    t2.metric("Queue/orchestration gap", f"{gap_total:.2f}s")
                    t3.metric("Estimated wall-time", f"{wall_est:.2f}s")
                    st.dataframe(pd.DataFrame(timeline_rows), use_container_width=True, hide_index=True)

                    # Steps table
                    rows_data = []
                    for s in steps:
                        svc = s.get("service_name", "?")
                        svc_type_icon = "📥" if "extract" in svc else ("📤" if "load" in svc else "⚙️")
                        rows_out = s.get("rows_out", s.get("rows_in", "—"))
                        extra = ""
                        if "removed_outliers" in s:
                            extra = f"removed {s['removed_outliers']} outliers"
                        elif "dq_checks" in s:
                            checks = s["dq_checks"]
                            passed = sum(
                                1
                                for v in checks.values()
                                if v is True or (isinstance(v, dict) and v.get("pass"))
                            )
                            extra = f"{passed}/{len(checks)} checks passed"
                        elif "output_file" in s:
                            extra = Path(s["output_file"]).name
                        elif "file_path" in s:
                            extra = Path(s["file_path"]).name
                        elif s.get("strategy"):
                            extra = f"strategy={s['strategy']}"

                        rows_data.append({
                            "": svc_type_icon,
                            "Service": svc,
                            "Duration": f"{s.get('duration_sec', 0):.3f}s",
                            "Rows": str(rows_out),
                            "Detail": extra,
                        })

                    st.dataframe(pd.DataFrame(rows_data), use_container_width=True, hide_index=True)

    # ─── Tab 3: Raw metadata (for debugging) ───
    with tab_meta:
        if not metadata_items:
            st.info("No metadata files.")
        else:
            st.caption(f"{len(metadata_items)} metadata file(s)")
            for m in metadata_items[:20]:  # cap at 20 to avoid slowness
                svc = m.get("service_name", "unknown")
                ts = m.get("timestamp", "?")
                with st.expander(f"{svc} — {ts}"):
                    st.json(m)


def _default_pipeline_yaml() -> str:
    return """pipeline:
  name: my_pipeline
  description: Describe your pipeline here
  steps:
    - id: extract
      service: extract_csv
      params:
        file_path: /app/data/myfile.csv
    - id: quality
      service: data_quality
      params:
        rules:
          min_rows: 10
          check_null_ratio: true
          threshold_null_ratio: 0.5
      depends_on: [extract]
    - id: clean
      service: clean_nan
      depends_on: [quality]
    - id: save
      service: load_data
      params:
        format: csv
      depends_on: [clean]
"""



# ── Main App ──
def main():
    init_session_state()
    render_sidebar()

    tab1, tab2, tab3, tab4 = st.tabs(["📝 Pipeline Editor", "🚀 Execution", "📂 Datasets", "📚 Services"])

    with tab1:
        render_pipeline_editor()
    with tab2:
        render_execution_panel()
    with tab3:
        render_dataset_explorer()
    with tab4:
        render_service_catalog()


if __name__ == "__main__":
    main()
