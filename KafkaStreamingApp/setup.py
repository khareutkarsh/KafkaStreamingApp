from setuptools import setup, find_packages

setup(
    name="KafkaStreamingApp",
    version="1.0.0",
    packages=find_packages(),
	data_files=[('', ['__main__.py', ])]
)
