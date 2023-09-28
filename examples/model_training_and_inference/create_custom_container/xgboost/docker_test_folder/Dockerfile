# SageMaker XGBoost image
# Check their implementation in https://github.com/aws/sagemaker-xgboost-container
FROM 683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.5-1

ENV PATH="/opt/ml/code:${PATH}"

# this environment variable is used by the SageMaker prebuilt container to determine our user code directory.
ENV SAGEMAKER_SUBMIT_DIRECTORY /opt/ml/code

# /opt/ml and all subdirectories are utilized by SageMaker, use the /code subdirectory to store your user code.
COPY code/* /opt/ml/code/

RUN pip --no-cache-dir install --upgrade pip

RUN pip --no-cache-dir install dask[complete]

RUN pip install sagemaker-training

# Defines train.py as script entrypoint
ENV SAGEMAKER_PROGRAM train.py