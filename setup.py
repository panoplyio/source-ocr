from distutils.core import setup

setup(
    name='panoply_ocr',
    version='2.0.0',
    description='Panoply Data Source for One Click Retail',
    author='Lior Rozen',
    author_email='lior@panoply.io',
    url='http://panoply.io',
    install_requires=[
        'panoply-python-sdk==1.3.2',
        'mock==2.0.0',
    ],

    # place this package within the panoply package namespace
    package_dir={'panoply': ''},
    packages=['panoply.ocr']
)
