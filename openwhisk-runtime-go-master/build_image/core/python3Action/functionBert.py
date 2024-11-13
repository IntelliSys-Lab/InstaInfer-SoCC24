import sys, os, datetime, warnings


warnings.filterwarnings("ignore")


def main():
    load_lib_begin = datetime.datetime.now()
    import torch
    from transformers import BertTokenizer, BertModel, BertConfig
    load_lib_end = datetime.datetime.now()

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

    lib_load_time = (load_lib_end - load_lib_begin) / datetime.timedelta(milliseconds=1)
    model_load_time = (load_model_end - load_model_begin) / datetime.timedelta(milliseconds=1)
    input_processing_time = (input_processing_end - process_begin) / datetime.timedelta(milliseconds=1)
    inference_time = (inference_end - inference_begin) / datetime.timedelta(milliseconds=1)
    process_time = (process_end - load_lib_begin) / datetime.timedelta(milliseconds=1)

    is_cold = 0
    fname = "cold_run"
    if not os.path.exists(fname):
        is_cold = 1
        open(fname, "a").close()

    return {
        'result': {'output': ' '},
        'measurement': {
            'lib_load_time': lib_load_time,
            'model_load_time': model_load_time,
            'input_processing_time': input_processing_time,
            'inference_time': inference_time,
            'compute_time': process_time,
            'model': 'bert-base',
            'is_cold?': is_cold,
            'is_load?': 0,
        }
    }


if __name__ == "__main__":
    print(main())