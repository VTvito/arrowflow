"""
ETL Pipeline Builder — Streamlit Application

AI-assisted interface for building, visualizing, and executing ETL pipelines
using the microservices platform.

Run: streamlit run streamlit_app/app.py
"""

import json
import os
import sys
import time

import pandas as pd
import pyarrow.ipc as pa_ipc
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
            ["openai", "local"],
            help="OpenAI requires OPENAI_API_KEY env var. Local uses the HuggingFace service.",
        )

        if provider == "openai":
            api_key = st.text_input("OpenAI API Key", type="password", value=os.getenv("OPENAI_API_KEY", ""))
            if api_key:
                os.environ["OPENAI_API_KEY"] = api_key

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


def render_execution_panel():
    st.header("🚀 Execution")

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

    tab1, tab2, tab3 = st.tabs(["📝 Pipeline Editor", "🚀 Execution", "📚 Services"])

    with tab1:
        render_pipeline_editor()
    with tab2:
        render_execution_panel()
    with tab3:
        render_service_catalog()


if __name__ == "__main__":
    main()
