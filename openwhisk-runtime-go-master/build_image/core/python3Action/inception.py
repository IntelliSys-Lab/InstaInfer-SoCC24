import sys, datetime,warnings
warnings.filterwarnings("ignore")
#from memory_profiler import profile
lib_start = datetime.datetime.now()
from PIL import Image
import torch
from torchvision import transforms
from torchvision.models import inception_v3
import boto3
import os
import json
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
    class_idx = json.load(open(os.path.join(SCRIPT_DIR, "imagenet_class_index.json"), 'r'))
    idx2label = [class_idx[str(k)][1] for k in range(len(class_idx))]

    image_path = os.path.join(SCRIPT_DIR,'beach.jpg')
    model_path = os.path.join(SCRIPT_DIR,'inception_v3.pth')

    model_process_begin = datetime.datetime.now()
    model = inception_v3(pretrained=False)
    model.load_state_dict(squirrel_load_model(model_path))
    model.eval()

    model_process_end = datetime.datetime.now()

    line = sys.stdin.readline()  # block until some input is received

    process_begin = datetime.datetime.now()
    input_image = Image.open(image_path)
    preprocess = transforms.Compose([
        transforms.Resize(299),  # Inception v3 requires the input size to be 299x299
        transforms.CenterCrop(299),
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

    # model_download_time = (model_download_end - model_download_begin) / datetime.timedelta(microseconds=1)
    lib_load_time = (lib_end - lib_start) / datetime.timedelta(milliseconds=1)
    model_load_time = (model_process_end - model_process_begin) / datetime.timedelta(milliseconds=1)
    #wait_time = (process_begin - model_end) / datetime.timedelta(milliseconds=1)
    model_process_time = (model_process_end - model_process_begin) / datetime.timedelta(milliseconds=1)
    process_time = (process_end - process_begin) / datetime.timedelta(milliseconds=1)

    image_time = (image_end - process_begin) / datetime.timedelta(milliseconds=1)
    unsquence_time = (inference_begin - image_end) / datetime.timedelta(milliseconds=1)
    inference_time = (inference_end - inference_begin) / datetime.timedelta(milliseconds=1)
    label_time = (process_end - inference_end) / datetime.timedelta(milliseconds=1)

    return {
        'result': {'idx': index.item(), 'class': ret},
        'measurement': {
            'lib_load_time_(supposed)': lib_load_time,
            'model_load_time_(supposed)': model_load_time,
            #'wait_time': wait_time,
            'compute_time': process_time,
            'model': 'inception_v3',
            'image_time':image_time,
            'unsquence_time':unsquence_time,
            'inference_time':inference_time,
            'label_time':label_time,
            'is_cold?': 0,
            'is_load?': 1,
        }
    }



if __name__ == "__main__":
    while True:
        result = function()
        print(result)
        sys.stdout.flush()