from setuptools import setup, find_packages

setup(
    name='profiling-tool',
    version='1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    url='',
    license='',
    author='PrudhvitejaCherukuri',
    author_email='',
    description='',
    install_requires=['pandas',
                      'numpy',
                      'scipy',
                      'scikit-learn',
                      'openpyxl',
                      'tqdm',
                      'pywin32',
                      'numerize',
                      'kmodes']
)
