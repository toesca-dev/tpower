from setuptools import setup, find_packages

setup(
    name='tpower',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    description='Python package by T-Power. \n\
        Interfaces with CEN API (https://www.coordinador.cl/desarrollo/documentos/actualizaciones-del-sistema-de-informacion-publica/api-publica-del-sip/), \
        Sistema de Medidas del Mercado Eléctrico Chileno - PRMTE (https://medidas.coordinador.cl/), \
        Green Power Monitor SCADA API (https://www.greenpowermonitor.com/gpm-scada/)',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Matías Galetovic Streeter',
    author_email='matias.galetovic@toesca.com',
    url='https://github.com/toesca-dev/tpower/tree/master/prmte',
    install_requires=[
        'requests', 'logging', 'pandas', 'datetime', 'dateutils'
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
