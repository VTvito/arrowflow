import logging

import torch
from transformers import pipeline

logger = logging.getLogger('text-completion-llm-service')

# Define the model and device for text generation
device = "cuda" if torch.cuda.is_available() else "cpu"
model_id = "meta-llama/Llama-3.2-1B-Instruct"

# Lazy-loaded model: initialized on first request, not at import time
_text_generator = None


def get_text_generator():
    """Load the HF text generation pipeline lazily on first call."""
    global _text_generator
    if _text_generator is None:
        logger.info(f"Loading model '{model_id}' on device '{device}'...")
        _text_generator = pipeline(
            "text-generation", model=model_id,
            device=device, torch_dtype=torch.bfloat16
        )
        logger.info("Model loaded successfully.")
    return _text_generator


def fill_missing_text(df, text_col, max_tokens, missing_placeholder, max_rows=None):
  """
  Searches for a specified placeholder (e.g. "[MISSING]") in the `text_col` column and generates completions for it.

  Parameters:
    - df (pd.DataFrame): Input DataFrame
    - text_col (str): Name of the column containing texts
    - max_tokens (int): Maximum number of tokens to generate
    - missing_placeholder (str): The placeholder to search for (e.g. "[MISSING]")
    - max_rows (int|None): Maximum number of rows to process. None = all rows.

  Returns:
    - (modified_df, number_of_replacements, prompt_template)
  """
  if text_col not in df.columns:
    logger.warning(f"Column '{text_col}' not found => skipping text completion.")
    return (df, 0, None)

  text_gen = get_text_generator()

  prompt_template = ("Read the the following text and replace {placeholder} with another natural word or phrase:\n"
                    "Text: {input_text}\n"
                    "Completion:")

  total_completed = 0
  rows_processed = 0
  row_limit = max_rows if max_rows is not None else float('inf')

  for idx, row in df.iterrows():
    text = row[text_col]
    if not isinstance(text, str):
      continue

    if missing_placeholder in text and rows_processed < row_limit:
      # Process placeholders in actual row
      while missing_placeholder in text:
        prompt = prompt_template.format(input_text=text, placeholder=missing_placeholder)
        generated = text_gen(prompt, max_new_tokens=max_tokens, temperature=0.5)[0]['generated_text']
        # Remove the prompt to obtain only the completion text
        completion_str = generated.replace(prompt, "").strip()
        text = text.replace(missing_placeholder, completion_str, 1)
        total_completed += 1
      rows_processed += 1

    df.at[idx, text_col] = text

  return (df, total_completed, prompt_template)
