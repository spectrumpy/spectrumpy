from setuptools import Extension, find_packages, setup

setup(name='spectrumpy',
      version='0.1',
      description='Client integration of Pitney Bowes Spectrum Technology Platform for Python',
      url='https://github.com/PitneyBowes/spectrumpy',
      author='Cary Peebles',
      author_email='cary.peebles@pb.com',
      license='Apache License 2.0',
      packages=find_packages("src"),
      package_dir={"": "src"},
      include_package_data=True,
      install_requires=[
            'requests',
            'zeep',
            'lxml',
            'configparser',
            'datetime'
      ],
      zip_safe=False)
