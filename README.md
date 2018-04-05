# NileDB Nifi

![logo](logo.png)

Welcome to [NileDB](https://niledb.com) Nifi, a series of NiFi processors that facilitates ingestion of data into NileDB Core platform.

Feel free to give us your feedback in order to improve the available features. Thanks.

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Get%20NileDB,%20the%20open-source%20Data%20Backend!&url=https://niledb.com)

### Build

In order to build NileDB Nifi, you need [Java](https://www.java.com/en/download/) to be installed previously.

First of all, you need to download the project:

    > git clone https://github.com/NileDB/com.niledb.nifi.git
    > cd com.niledb.nifi

Now, you can build the project:

    > ./gradlew build

Now, you can copy the NAR plugin file from build/libs to your NiFi installation. If you are using docker-compose with NileDB Core, you can issue the following command:

    > ./deploy.sh

### Code of Conduct

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md).
By contributing to this project (commenting or opening PR/Issues etc) you are agreeing to follow this conduct, so please
take the time to read it. 

### Acknowledgments

This product uses and is based on the following:
* [Apache NiFi](https://nifi.apache.org/)
* [Vert.x](http://vertx.io/)
* [OpenJDK](http://openjdk.java.net/)

### License

NileDB Nifi is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE.txt) for details.

Copyright (c) 2018, NileDB, Inc.

[NileDB Nifi License](LICENSE.txt)
