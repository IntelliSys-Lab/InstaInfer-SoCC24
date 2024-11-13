# How to Modify the Runtime Proxy

This repo is used for building OpenWhisk custom image for [InstaInfer](https://github.com/IntelliSys-Lab/InstaInfer-SoCC24). There are two steps for building the runtime image.

### Step 1. Build the Proxy

Please refer to the [openwhisk folder](https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui/tree/main/openwhisk) that holds the proxy project (written in GO). InstaInfer mainly modified XXXHandler (like loadHandler) and ActionProxy.

### Step 2. Build the Runtime Image

After modifying the proxy code, we can now build the proxy into Docker image. Please refer to [How to build Runtime Image](https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui/tree/main/build_image) for details.