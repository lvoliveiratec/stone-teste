#!/bin/bash

# 1. Autenticar no Amazon ECR
echo "Autenticando no Amazon ECR..."
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 885394832474.dkr.ecr.us-east-1.amazonaws.com
if [ $? -ne 0 ]; then
    echo "Falha na autenticação com o ECR."
    exit 1
fi

# 2. Criar uma Imagem Docker
echo "Construindo a imagem Docker..."
sudo docker build -t extract-cnpjs .
if [ $? -ne 0 ]; then
    echo "Falha na criação da imagem Docker."
    exit 1
fi

# 3. Fazer o Tag da Imagem Docker
echo "Tagueando a imagem Docker..."
docker tag extract-cnpjs:latest 885394832474.dkr.ecr.us-east-1.amazonaws.com/stn-images/extract-cnpjs:latest
if [ $? -ne 0 ]; then
    echo "Falha no tag da imagem Docker."
    exit 1
fi

# 4. Fazer Push da Imagem Docker
echo "Enviando a imagem Docker para o ECR..."
docker push 885394832474.dkr.ecr.us-east-1.amazonaws.com/stn-images/extract-cnpjs:latest
if [ $? -ne 0 ]; then
    echo "Falha ao enviar a imagem Docker para o ECR."
    exit 1
fi

echo "Processo concluído com sucesso!"
