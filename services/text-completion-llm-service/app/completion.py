import logging
import re
import pandas as pd
from transformers import pipeline
import torch


logger = logging.getLogger('text-completion-llm-service')

# Define the model and device for text generation
device = "cuda" if torch.cuda.is_available() else "cpu"
model_id = "meta-llama/Llama-3.2-1B-Instruct"

# Load the HF text generation pipeline (the model is pre-downloaded)
text_generator = pipeline("text-generation", model=model_id,
                           device=device, torch_dtype=torch.bfloat16)

def fill_missing_text(df, text_col, max_tokens, missing_placeholder):
  """
  Searches for a specified placeholder (e.g. "[MISSING]") in the `text_col` column and generates completions for it.
  
  Parameters:
    - df (pd.DataFrame): Input DataFrame
    - text_col (str): Name of the column containing texts
    - max_tokens (int): Maximum number of tokens to generate
    - missing_placeholder (str): The placeholder to search for (e.g. "[MISSING]")
  
  prompt_template (str): Template for the prompt. It should include {input_text} and {placeholder}.
  
  Returns:
    - (modified_df, number_of_replacements, prompt_template)
      modified_df: DataFrame in which each occurrence (in the first 5 rows encountered) of the placeholder is replaced by model-generated text
      number_of_replacements: Total number of placeholders replaced
      prompt_template: The prompt template used for generation
       
  Note: If the column `text_col` does not exist or there are no strings containing the placeholder,
  the function returns (df, 0, prompt_template).
  """
  if text_col not in df.columns:
    logger.warning(f"Column '{text_col}' not found => skipping text completion.")
    return (df, 0, None)

  prompt_template = ("Read the the following text and replace {placeholder} with another natural word or phrase:\n"
                    "Text: {input_text}\n"
                    "Completion:")

  total_completed = 0
  rows_processed = 0
  for idx, row in df.iterrows():
    text = row[text_col]
    if not isinstance(text, str):
      continue

    if missing_placeholder in text and rows_processed < 5:
      # Process placeholders in actual row
      while missing_placeholder in text:
        prompt = prompt_template.format(input_text=text, placeholder=missing_placeholder)
        generated = text_generator(prompt, max_new_tokens=max_tokens, temperature=0.5)[0]['generated_text']
        # Remove the prompt to obtain only the completion text
        completion_str = generated.replace(prompt, "").strip()
        text = text.replace(missing_placeholder, completion_str, 1)
        total_completed += 1
      rows_processed += 1

    df.at[idx, text_col] = text

  return (df, total_completed, prompt_template)