from setuptools import setup as _setup
from setuptools import find_packages

import app as app
import app.distance_calculator as distance_calculator

def _requirements():
    return [r for r in open('requirements.txt')]

def setup():
    _setup(name=app.__name__,
           version='0.0.1',
           description="""Calculate Distance to streets""",
           url='https://github.com/emptymalei/geoeconomics-with-data',
           author='Lei Ma',
           author_email='leima137@gmail.com',
           license='MIT',
           packages= find_packages(),
           entry_points={
               'console_scripts': [
                   'distance_calculator = {}:main'.format(distance_calculator.__name__),
               ],
           },
           include_package_data=True,
           zip_safe=False
    )


if __name__ == '__main__':
    setup()
