from setuptools import setup, find_packages

setup(
    name='tpower',
    version='0.1.5',
    packages=find_packages(),
    include_package_data=True,
    description='Python package by T-Power.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Matías Galetovic Streeter',
    author_email='matias.galetovic@toesca.com',
    url='https://github.com/toesca-dev/tpower/',
    install_requires=[
        'requests', 'pandas', 'numpy'
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
