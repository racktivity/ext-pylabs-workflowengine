## ---------------------------------------------------------
## Qpackage config
#QPDOMAIN=cloud.aserver.com
#QPNAME=cloud_ras_server
#QPVERSION=0.1
##QPDEPENDENCY=['pexpect','pylabs.org',None,'','','generic']
#
## ---------------------------------------------------------
## Qpackage rules
#QR=/opt/qbase3/var/qpackages/${QPDOMAIN}/${QPNAME}/${QPVERSION}/upload_trunk/
#QT=${QR}tasklets/generic/
#QF=${QR}files/generic/
#true:
#	true
#create:
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.create('${QPNAME}','${QPVERSION}','${QPDOMAIN}','trunk','generic')"
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='NEW').addDependency(*${QPDEPENDENCY})"
#	sudo rsync -rtv qpackage/ ${QT}
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='NEW').source.export()"
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='NEW').package()"
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='NEW').install()"
#createpub:
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='NEW').publish()"
#update:
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='LOCAL').prepare()"
#	sudo rsync -rtv qpackage/ ${QT}
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='MOD').source.export()"
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='MOD').package()"
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='MOD').install()"
#updatepub:
#	sudo /opt/qbase3/qshell -c "q.qshellconfig.interactive=True;i.qpackages.findFirst('${QPNAME}','${QPVERSION}',state='MOD').publish()"
#
## ---------------------------------------------------------
#commit:
#	echo hg commit -m "`hg tip --template={desc}`";
#	echo hg push
#	echo make update
#	echo make updatepub

test:
#	sudo rsync -rtvn --size-only ./apps/ /opt/qbase3/apps/
#	sudo rsync -rtvn --size-only ./lib/ /opt/qbase3/lib/
	sudo rsync -rtv ./apps/ /opt/qbase3/apps/
	sudo rsync -rtv ./lib/ /opt/qbase3/lib/
	sudo /opt/qbase3/qshell -c 'q.manage.applicationserver.stop()'
	sudo /opt/qbase3/qshell -c 'q.manage.applicationserver.start()'
	sudo /opt/qbase3/qshell -c 'q.manage.workflowengine.stop()'
	sudo /opt/qbase3/qshell -c 'q.manage.workflowengine.start()'
##	sudo tail -f /opt/qbase3/var/log/workflowengine.stdout /opt/qbase3/var/log/workflowengine.stderr &
#	sudo /opt/qbase3/qshell -c 'print i.config.cloudApiConnection.find("main").application.addAccount("","","","")'
#	sudo /opt/qbase3/qshell
	./test.py

