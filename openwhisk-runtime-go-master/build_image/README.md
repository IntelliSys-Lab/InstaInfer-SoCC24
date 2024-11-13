# How to build Runtime Image

This repo is used for building OpenWhisk custom image for InstaInfer. To create the image of InstaInfer, please run the [build.sh](https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui/blob/main/build_image/build.sh) script directly.

**Notice:**
The model file like resnet152.pth has been removed from the repo. To build the runtime that can achieve pre-loading, please move your model file to [core/python3Action](https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui/tree/main/build_image/core). See build details in [Dockerfile](https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui/blob/main/build_image/core/python3Action/Dockerfile).