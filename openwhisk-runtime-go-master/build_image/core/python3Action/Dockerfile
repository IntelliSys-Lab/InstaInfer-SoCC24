#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# build go proxy from source
FROM golang:1.15 AS builder_source


ARG GO_PROXY_GITHUB_BRANCH=main

RUN git clone --branch ${GO_PROXY_GITHUB_BRANCH} \
   https://github.com/IntelliSys-Lab/openwhisk-runtime-go-sui /src ;\
   cd /src ; env GO111MODULE=on CGO_ENABLED=0 go build main/proxy.go && \
   mv proxy /bin/proxy

FROM suiyifan/python3:v1

# select the builder to use
ARG GO_PROXY_BUILD_FROM=source

# Install common modules for python
#COPY requirements.txt requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /action
RUN mkdir -p /_test


ADD loadalex.sh /_test/loadalex.sh
RUN chmod +x /_test/loadalex.sh
ADD loadvgg.sh /_test/loadvgg.sh
RUN chmod +x /_test/loadvgg.sh
ADD loadinception.sh /_test/loadinception.sh
RUN chmod +x /_test/loadinception.sh
ADD loadres18.sh /_test/loadres18.sh
RUN chmod +x /_test/loadres18.sh
ADD loadres50.sh /_test/loadres50.sh
RUN chmod +x /_test/loadres50.sh
ADD loadres152.sh /_test/loadres152.sh
RUN chmod +x /_test/loadres152.sh
ADD loadgooglenet.sh /_test/loadgooglenet.sh
RUN chmod +x /_test/loadgooglenet.sh
ADD loadbert.sh /_test/loadbert.sh
RUN chmod +x /_test/loadbert.sh


ADD funcalex.sh /_test/funcalex.sh
RUN chmod +x /_test/funcalex.sh
ADD funcvgg.sh /_test/funcvgg.sh
RUN chmod +x /_test/funcvgg.sh
ADD funcinception.sh /_test/funcinception.sh
RUN chmod +x /_test/funcinception.sh
ADD func18.sh /_test/func18.sh
RUN chmod +x /_test/func18.sh
ADD func50.sh /_test/func50.sh
RUN chmod +x /_test/func50.sh
ADD func152.sh /_test/func152.sh
RUN chmod +x /_test/func152.sh
ADD funcgooglenet.sh /_test/funcgooglenet.sh
RUN chmod +x /_test/funcgooglenet.sh
ADD funcbert.sh /_test/funcbert.sh
RUN chmod +x /_test/funcbert.sh

ADD alex.py /_test/alex.py
ADD vgg.py /_test/vgg.py
ADD inception.py /_test/inception.py
ADD res18.py /_test/res18.py
ADD res50.py /_test/res50.py
ADD res152.py /_test/res152.py
ADD googlenet.py /_test/googlenet.py
ADD bert.py /_test/bert.py

ADD functionAlex.py /_test/functionAlex.py
ADD functionVgg.py /_test/functionVgg.py
ADD functionInception.py /_test/functionInception.py
ADD function18.py /_test/function18.py
ADD function50.py /_test/function50.py
ADD function152.py /_test/function152.py
ADD functionGooglenet.py /_test/functionGooglenet.py
ADD functionBert.py /_test/functionBert.py

ADD imagenet_class_index.json /_test/imagenet_class_index.json
ADD vocab.txt /_test/vocab.txt

ADD alexnet.pth /_test/alexnet.pth
ADD vgg19.pth /_test/vgg19.pth
ADD inception_v3.pth /_test/inception_v3.pth
ADD resnet18.pth /_test/resnet18.pth
ADD resnet50.pth /_test/resnet50.pth
ADD resnet152.pth /_test/resnet152.pth
ADD googlenet.pth /_test/googlenet.pth
ADD bert-base-uncased.pth /_test/bert-base-uncased.pth

ADD beach.jpg /_test/beach.jpg


WORKDIR /
COPY --from=builder_source /bin/proxy /bin/proxy_source
RUN mv /bin/proxy_${GO_PROXY_BUILD_FROM} /bin/proxy
ADD bin/compile /bin/compile
ADD lib/launcher.py /lib/launcher.py

# log initialization errors
ENV OW_LOG_INIT_ERROR=1
# the launcher must wait for an ack
ENV OW_WAIT_FOR_ACK=1
# execution environment
ENV OW_EXECUTION_ENV=openwhisk/action-python-v3.7
# compiler script
ENV OW_COMPILER=/bin/compile

ENTRYPOINT ["/bin/proxy"]
