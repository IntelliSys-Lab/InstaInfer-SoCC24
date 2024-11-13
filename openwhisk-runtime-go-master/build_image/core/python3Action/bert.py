import sys, os, datetime, warnings
warnings.filterwarnings("ignore")
#from memory_profiler import profile
lib_start = datetime.datetime.now()
import torch
from transformers import BertTokenizer, BertModel, BertConfig
lib_end = datetime.datetime.now()

model_dict = {}

def squirrel_load_model(model_path):
    if 'model' in globals( ):
        return globals( ) ['model']
    else:
        model = torch.load(model_path)
        return model

#@profile
def function():
    SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))

    model_path = 'bert-base-uncased'
    saved_model_path = os.path.join(SCRIPT_DIR,'bert-base-uncased.pth')
    tokenizer_path = os.path.join(SCRIPT_DIR,'vocab.txt')
    input_text = "Here is an example sentence for the BERT model."

    load_model_begin = datetime.datetime.now()

    model = BertModel(BertConfig())
    model.load_state_dict(torch.load(saved_model_path))

    tokenizer = BertTokenizer.from_pretrained(tokenizer_path)
    load_model_end = datetime.datetime.now()

    line = sys.stdin.readline()  # block until some input is received

    process_begin = datetime.datetime.now()
    input_tokenized = tokenizer.encode_plus(
        input_text,
        add_special_tokens=True,
        max_length=512,
        pad_to_max_length=True,
        return_tensors='pt',
        truncation=True,
    )
    input_processing_end = datetime.datetime.now()

    inference_begin = datetime.datetime.now()
    output = model(**input_tokenized)
    inference_end = datetime.datetime.now()

    process_end = datetime.datetime.now()

    lib_load_time = (lib_end - lib_start) / datetime.timedelta(milliseconds=1)
    model_load_time = (load_model_end - load_model_begin) / datetime.timedelta(milliseconds=1)
    input_processing_time = (input_processing_end - process_begin) / datetime.timedelta(milliseconds=1)
    inference_time = (inference_end - inference_begin) / datetime.timedelta(milliseconds=1)
    process_time = (process_end - process_begin) / datetime.timedelta(milliseconds=1)

    return {
        'result': {'idx': ' '},
        'measurement': {
            'lib_load_time_(supposed)': lib_load_time,
            'model_load_time_(supposed)': model_load_time,
            'compute_time': process_time,
            'model': 'bert',
            'input_processing_time':input_processing_time,
            'inference_time':inference_time,
            'is_cold?': 0,
            'is_load?': 1,
        }
    }



if __name__ == "__main__":
    while True:
        result = function()
        print(result)
        sys.stdout.flush()