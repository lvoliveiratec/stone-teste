# Stone Teste

Este repositório contém a implementação de um processo de integração de dados utilizando tecnologias como Airflow, AWS e Python. O foco do projeto é a automação de consultas Athena e a manipulação de dados a partir de arquivos em formato CSV, com o objetivo de carregá-los em um banco de dados ou sistema de armazenamento.


![arquitetura drawio](https://github.com/user-attachments/assets/fa93ba2b-8b35-4403-afbd-21c2d4a7443b)


## Estrutura do Repositório

- **dags**: Contém as DAGs do Airflow que orquestram o processo de ETL.
- **docker_images/extract-cnpjs**: Contém a imagem Docker usada para extrair dados de CNPJs.
- **layer_processing**: Contém os scripts de processamento dos dados, incluindo limpeza e transformação.
- **.gitignore**: Arquivo para ignorar arquivos desnecessários no controle de versão.
- **README.md**: Este arquivo.

## Principais Funcionalidades

1. **Consultas Athena**: O projeto utiliza consultas Athena para extrair dados e os armazena em um bucket S3.
2. **Processamento e Transformação de Dados**: Após a extração, o projeto realiza o processamento e a transformação desses dados.
3. **Armazenamento e Carregamento**: Os resultados processados são armazenados em um sistema de armazenamento apropriado, como o S3.

## Como Rodar

Estou disponibilizando o teste via airflow para demonstrar todo o fluxo.
Me avisem para que eu possa subir o ambiente, e acessem através do link > http://3.224.116.53:8080/home
**user:admin**
**senha:admin**

![image](https://github.com/user-attachments/assets/ff896dd3-f5c6-4a6a-b1f7-b2a407bfd2af)


![image](https://github.com/user-attachments/assets/b17fef68-d0f6-45fc-b78e-ad9b58a25c96)

![image](https://github.com/user-attachments/assets/000e63bc-0f01-4981-bfc5-bf65798dd5d5)


### Resultado Final

![image](https://github.com/user-attachments/assets/0b6d37ac-34ae-45d2-b349-831392c01969)


