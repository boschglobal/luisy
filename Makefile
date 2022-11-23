# Makefile for luisy

requirements:
	: # Update requirements_dev.txt if only new library is added
	: # Assumes that pip-tools is installed
	pip-compile requirements_dev.in --annotation-style line --no-emit-index-url --allow-unsafe


update-requirements:
	: # Update requirements_dev.txt if dependencies should be updated
	: # Assumes that pip-tools is installed
	pip-compile requirements_dev.in --annotation-style line --upgrade --no-emit-index-url --allow-unsafe
