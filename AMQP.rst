AMQP Changes
============

Extra Dependencies
------------------

* txAMQP__
* AMQP 0.8 XML specification_ on /opt/qbase5/cfg/amqp/amqp0-8.xml 

.. _txAMQP: http://pypi.python.org/packages/source/t/txAMQP/txAMQP-0.5.tar.gz#md5=ceac5960feec83d8b29dd03e85e8d552
.. _specification: http://py-amqplib.googlecode.com/hg-history/3a1f3d3f2cedc2ef7adee30ad17f2911748ca763/extras/amqp0-8.xml

Known issues
------------

TODO
----
  
* Fix known issues
 
* Make params confgurable for:

  * CloudAPIActionManager.WFLActionManager:
    - *getID*: should return a unique ID for this appserver instance (cfg file)
    - *getRoutingKey*: should be configurable as a tasklet as this will contain
      the logic to determine the correct routing key (i.e. determine the target WFE)

  * QueueInfrastructure.getAmqpConfig:
    Retrieve AMQP broker info from a cfg file
    
* The codebase could use some clean-up and harden love
