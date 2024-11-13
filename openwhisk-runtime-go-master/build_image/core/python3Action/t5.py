import time

# Measure time for loading libraries
start_time = time.time()
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

library_loading_time = time.time() - start_time

# Measure time for loading the model
start_time = time.time()
model_name = "t5-large"
tokenizer = T5Tokenizer.from_pretrained(model_name)
model = T5ForConditionalGeneration.from_pretrained(model_name)


# Prepare the device
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)
model.eval()
model_loading_time = time.time() - start_time

# Function to perform inference
def perform_inference(input_text):

    with torch.no_grad():
        # Encode input context
        input_ids = tokenizer.encode(input_text, return_tensors="pt").to(device)

        # Measure inference time
        start_time = time.time()
        outputs = model.generate(input_ids, max_length=512)
        inference_time = time.time() - start_time

        # Decode and return the output
        return tokenizer.decode(outputs[0]), inference_time


# Example input
input_text = "translate English to German: How are you today?"
output_text, inference_time = perform_inference(input_text)

# Print outputs and time measurements
print(f"Output: {output_text}")
print(f"Library Loading Time: {library_loading_time:.6f} seconds")
print(f"Model Loading Time: {model_loading_time:.6f} seconds")
print(f"Inference Time: {inference_time:.6f} seconds")