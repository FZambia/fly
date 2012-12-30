import os
import sys
import distutils.core



if sys.argv[-1] == 'test':
    status = os.system('python fly/tests.py')
    sys.exit(1 if status > 127 else status)


distutils.core.setup(
    name='fly',
    version="0.0.1",
    description="Modify your dictionaries (or objects instances) on the fly using json configuration",
    url='https://github.com/FZambia/aria',
    download_url='https://github.com/FZambia/aria',
    author="Alexandr Emelin",
    author_email='frvzmb@gmail.com',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    keywords="python dict object json",
    packages=['fly'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ],
)
