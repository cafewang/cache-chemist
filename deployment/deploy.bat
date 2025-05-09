k3d cluster create -p "8081:30081@agent:0" -p "8082:30082@agent:0" --agents 2
kubectl apply -f redis-config.yaml
kubectl apply -f redis-deployment.yaml
kubectl apply -f mysql.yaml