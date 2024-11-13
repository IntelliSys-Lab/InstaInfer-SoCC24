git init               
git add .
git commit -m "InstaInfer SoCC24"
git branch -M main
git remote rm origin

git remote add origin git@github.com:IntelliSys-Lab/openwhisk-runtime-go-sui.git

sleep 1
git push -u origin main

