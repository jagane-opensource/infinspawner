from setuptools import setup

setup(
    name='infinspawner',
    version='0.2',
    description='InfinStor spawner for JupyterHub',
    url='https://github.com/yuvipanda/simplespawner',
    author='InfinStor, Inc.',
    author_email='support@infinstor.com',
    license='3 Clause BSD',
    packages=['infinspawner'],
    entry_points={
        'jupyterhub.spawners': [
            'infinspawner = infinspawner:InfinSpawner',
        ],
    },
)
