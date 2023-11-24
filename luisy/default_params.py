# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

default_params = {
    'download': False,
    'upload': False,
    'dry_run': False,
    'working_dir': None,
    'azure_storage_key': None,
    'azure_account_name': None,
    'azure_container_name': None,
    'no_ask': False,
    'requirements_path': None,
    'hash_update_mode': False,
    'spark': None,
}
env_keys = {
    'working_dir': 'WORKING_DIR',
    'requirements_path': 'LUISY_REQUIREMENTS_PATH',
    'azure_storage_key': 'LUISY_AZURE_STORAGE_KEY',
    'azure_account_name': 'LUISY_AZURE_ACCOUNT_NAME',
    'azure_container_name': 'LUISY_AZURE_CONTAINER_NAME',
}
