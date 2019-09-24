# Thinking Data Analytics

This is the official Python SDK for Thinking Data Analytics.

## Easy Installation

You can get Thinking Data Analytics SDK using pip.

pip install ThinkingDataSdk

Once the SDK is successfully installed, use the TGA SDK likes:

python
import tgasdk

tga = TGAnalytics(LoggingConsumer("F:/home/sdk/log"))
tga.track('dis',None,"shopping",properties)
tga.flush()
tga.close()

## To learn more

See our [full manual](http://doc.thinkinggame.cn/tgamanual/installation/python_sdk_installation.html)

