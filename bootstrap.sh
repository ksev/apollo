echo "Starting bootstraping script"

HOME="/home/vagrant"

JDK_NAME="jdk1.7.0_25"
JDK_URL="http://download.oracle.com/otn-pub/java/jdk/7u25-b15/jdk-7u25-linux-x64.tar.gz"
JDK_DIR="/opt/$JDK_NAME"

echo "Installing Git"

sudo apt-get -y install git > /dev/null

echo "Installing JDK 7 (download is ~100MB so it might take a while)"

wget --no-cookies --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com" --no-check-certificate $JDK_URL -O jdk7.tar.gz &> /dev/null

sudo tar zxf $HOME/jdk7.tar.gz -C /opt/ > /dev/null
rm $HOME/jdk7.tar.gz > /dev/null

sudo update-alternatives --install /usr/bin/java java $JDK_DIR/bin/java 20000 > /dev/null
sudo update-alternatives --install /usr/bin/javac javac $JDK_DIR/bin/javac 20000 > /dev/null

echo "Installing SBT"

wget http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt/0.12.4/sbt.tgz &> /dev/null

sudo tar zxf $HOME/sbt.tgz -C /opt/ > /dev/null
rm $HOME/sbt.tgz

sudo update-alternatives --install /usr/bin/sbt sbt /opt/sbt/bin/sbt 20000 > /dev/null

echo "Installing CCM"

wget http://mirrors.gigenet.com/apache/ant/binaries/apache-ant-1.9.2-bin.tar.gz &> /dev/null

sudo tar zxf $HOME/apache-ant-1.9.2-bin.tar.gz -C /opt/ > /dev/null
rm $HOME/apache-ant-1.9.2-bin.tar.gz

sudo update-alternatives --install /usr/bin/ant ant /opt/apache-ant-1.9.2/bin/ant 20000 > /dev/null

sudo apt-get install -y python-pip > /dev/null
sudo pip install cql PyYAML > /dev/null

git clone https://github.com/pcmanus/ccm.git > /dev/null

cd ccm
sudo ./setup.py install > /dev/null
cd $HOME

echo "Setting up Cassandra cluster"

ccm create -vnodes --nodes 3 --cassandra-version 2.0.0-beta2 apollo

echo "DONE"
