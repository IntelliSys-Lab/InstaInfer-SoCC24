import datetime, json, os, uuid, sys
import time
import warnings
import logging

warnings.filterwarnings("ignore")

def main():
    logging.getLogger().setLevel(logging.INFO)
    # 调用function
    process_begin = datetime.datetime.now()
    from PIL import Image
    import torch
    from torchvision import transforms
    from torchvision.models import alexnet
    import boto3
    lib_end = datetime.datetime.now()

    SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    class_idx = json.load(open(os.path.join(SCRIPT_DIR, "imagenet_class_index.json"), 'r'))
    idx2label = [class_idx[str(k)][1] for k in range(len(class_idx))]

    image_path = os.path.join(SCRIPT_DIR,'beach.jpg')
    model_path = os.path.join(SCRIPT_DIR,'alexnet.pth')

    global model
    model = None



    model_process_begin = datetime.datetime.now()
    model = alexnet(pretrained=False)

    model.load_state_dict(torch.load(model_path))
    model.eval()
    model_end = datetime.datetime.now()

    input_image = Image.open(image_path)
    image_begin = datetime.datetime.now()
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

    image_end = datetime.datetime.now()

    input_batch = preprocess(input_image).unsqueeze(0)  # create a mini-batch as expected by the model
    inference_begin = datetime.datetime.now()
    output = model(input_batch)

    inference_end = datetime.datetime.now()

    _, index = torch.max(output, 1)
    # The output has unnormalized scores. To get probabilities, you can run a softmax on it.
    prob = torch.nn.functional.softmax(output[0], dim=0)
    _, indices = torch.sort(output, descending=True)
    ret = idx2label[index]
    process_end = datetime.datetime.now()

    lib_load_time = (lib_end - process_begin) / datetime.timedelta(milliseconds=1)
    model_load_time = (model_end - lib_end) / datetime.timedelta(milliseconds=1)
    wait_time = (model_end - process_begin) / datetime.timedelta(milliseconds=1)
    model_process_time = (model_end - model_process_begin) / datetime.timedelta(milliseconds=1)
    process_time = (process_end - process_begin) / datetime.timedelta(milliseconds=1)

    image_time = (image_end - image_begin) / datetime.timedelta(milliseconds=1)
    unsquence_time = (inference_begin - image_end) / datetime.timedelta(milliseconds=1)
    inference_time = (inference_end - inference_begin) / datetime.timedelta(milliseconds=1)
    label_time = (process_end - inference_end) / datetime.timedelta(milliseconds=1)

    is_cold = 0
    fname = "cold_run"
    if not os.path.exists(fname):
        is_cold = 1
        open(fname, "a").close()

    return {
        'result': {'idx': index.item(), 'class': ret},
        'measurement': {
            'lib_load_time': lib_load_time,
            'model_load_time': model_load_time,
            'wait_time': wait_time,
            'compute_time': process_time,
            'image_time':image_time,
            'unsquence_time':unsquence_time,
            'inference_time':inference_time,
            'label_time':label_time,
            'model': 'alexnet',
            'is_cold?': is_cold,
            'is_load?': 0,
        }
    }

if __name__ == "__main__":
    print(main())