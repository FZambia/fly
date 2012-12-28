import os
import sys
import distutils.core


# True if we are running on Python 3.
PY3 = sys.version_info[0] == 3


if sys.argv[-1] == 'test':
    interpreter = "python"
    if PY3:
        interpreter += "3" 
    status = os.system('%s arya/tests.py' % interpreter)
    sys.exit(1 if status > 127 else status)


distutils.core.setup(
    name='arya',
    version="0.0.1",
    description="Modify your dictionaries (or objects instances) on the fly using json configuration",
    url='https://github.com/FZambia/aria',
    download_url='https://github.com/FZambia/aria',
    author="Alexandr Emelin",
    author_email='frvzmb@gmail.com',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    keywords="python dict object json",
    packages=['arya'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
    ],
)