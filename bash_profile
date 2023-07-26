# Setting PATH for Python 3.10
# The original version is saved in .bash_profile.pysave
export NVM_DIR=~/.nvm
source $(brew --prefix nvm)/nvm.sh
PATH="/Library/Frameworks/Python.framework/Versions/3.10/bin/python3.10:${PATH}"
export PATH

export PYTHONPATH=$PYTHONPATH:/Library/Frameworks/Python.framework/Versions/3.10/site-packages



export SPARK_HOME=~/Downloads/spark-2.3.2-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH
#export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home
#For python 3, You have to add the line below or you will get an error
export PYSPARK_PYTHON=python3
export PATH=$JAVA_HOME/bin:$PATH
export PATH

## HIVE env variables
export HIVE_HOME=/usr/local/Cellar/hive/3.1.3/libexec
export PATH=$PATH:/$HIVE_HOME/bin

## MySQL ENV
export PATH=$PATH:/usr/local/mysql-8.0.12-macos10.13-x86_64/bin



# Setting PATH for Python 3.7
# The original version is saved in .bash_profile.pysave
PATH="/Library/Frameworks/Python.framework/Versions/3.9/bin:${PATH}"
export PATH
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
