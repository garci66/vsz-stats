import pandas as pd
import dask.dataframe as dd
a = pd.DataFrame([[None, 'Hola'],[None, 'Chau']], columns=(['valor','nombre']))
b = pd.DataFrame([['::1', 'Hola'],['::1', 'Chau']], columns=(['valor','nombre']))

parquetfile='./tmp/three.parquet'

a.to_parquet(parquetfile, engine='fastparquet', append=False, compression='GZIP')
b.to_parquet(parquetfile, engine='fastparquet', append=True, compression='GZIP')




import s3fs
from fastparquet import ParquetFile
thiss3 = s3fs.S3FileSystem()
myopen = thiss3.open
pf = ParquetFile(parquetfile, open_with=myopen)
df = pf.to_pandas()




pf = ParquetFile(parquetfile)
df = pf.to_pandas()



a = pd.DataFrame([[None, 'Hola','::1',1],[None, 'Chau','::2',2]], columns=(['ipv6Addr','nombre','direccionV6','valor']))
b = pd.DataFrame([['2001::1', 'Hola','::1',1],['::10', 'Chau','::2',2]], columns=(['ipv6Addr','nombre','direccionV6','valor']))

parquetfile='./tmp/v6v3.parquet'

v6cols=a.columns[a.columns.str.contains('v6',case=False, regex=False)]
v6colsObj=dict((el,'bytes') for el in v6cols)


a.to_parquet(parquetfile, engine='fastparquet', append=False, compression='GZIP', has_nulls=False)
b.to_parquet(parquetfile, engine='fastparquet', append=True, compression='GZIP', has_nulls=False)


a.to_parquet(parquetfile, engine='fastparquet', append=False, compression='GZIP')
b.to_parquet(parquetfile, engine='fastparquet', append=True, compression='GZIP')

import CstringIO


data_system='''apMac,sampleTime,ap,usbDeviceVersion,usbDeviceVID,usbDevicePID,gpsInfo,countryCode,timestamp,seqNumber,zone_id,zoneName,timeZone,gatewayIp,lastRebootReason,totalBootCount,mtuSize,rejoinCount,rejoinReason,oops,lossConnectBootCnt,deviceName,location,fwVersion,devSupportUsb,deviceIpMode,ip,ipv6,ipsecIp,apConnectedIp,uptime,mountState,currentTemperature,lifeMaxTemperature,lifeMinTemperature,dnatInfo,rksDpIp,rksDpIpOnly,ipType,isIpTypeChanged,managementVlan,apState,isConnectionTotalCountFlagged,totalConnectedClient,crashDump,altitudeUnit,altitudeValue,poeMode,poeModeSetting,ipv6Type,cpuPercentage,totalMemory,freeMemory,model,serialNumber,desc,numRadio,szConnCpIp,szConnCpIpv6,szConnDpIp,szConnDpIpv6,netmask,IpDnsSvr1,IpDnsSvr2,Ipv6DnsSvr1,Ipv6DnsSvr2,ApStatus,firstJoinTime,lastBootTime,lastConfSyncTime,freeStorage,ethPortStatus,ethStateChange,numRogues,numAuthClients,rxByteRate,txByteRate,rxErrorPkts,txErrorPkts,RxDropPkts,LanStatsRxBytes,LanStatsTxBytes,LanStatsRxPkts,LanStatsTxPkts,LanStatsRxErrorPkts,LanStatsTxErrorPkts,LanStatsRxBcastPkts,LanStatsTxBcastPkts,LanStatsRxMcastPkts,LanStatsTxMcastPkts,LanStatsRxUcastPkts,LanStatsTxUcastPkts,LanStatsRxDroppedPkts,LanStatsTxDroppedPkts,LanStatsRxByteRate,LanStatsTxByteRate,TxDropPkts
E8:1D:A8:22:A7:10,1525352429,E8:1D:A8:22:A7:10,,,,"-34.564651,-58.463316",AR,1525352433,2876,9aafc307-7d05-43c2-aecc-a9993bbcedb5,Belgrano,ART+3,192.168.100.1,power cycle,1,1500,9,"init socket failed,http request failed,",,4,AP-PR-01-31,Belgrano Day school,3.6.0.0.709,0,1,192.168.101.153,,,192.168.101.153,517589,,0,0,0,,,,dynamic,0,1,Flagged,false,1,0,,0,0,0,autoconf,20.0,254280,128796,R310,371709006615,Ruckus R310 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.254.0,192.168.0.5,8.8.8.8,,,1,0,0,0,5432,1,0,0,0,0,0,4107,1782493,0,1714251472,1915655341,12138787,6339306,482,0,0,0,0,0,0,0,9,0,0,0,0
E8:1D:A8:17:DF:E0,1525352446,E8:1D:A8:17:DF:E0,,,,"-25.596687,-54.545629",AR,1525352450,2889,000cbc03-192a-4e34-894f-3181b97fdf87,Iguazu,ART+3,192.168.128.1,power cycle,1,1500,5,"init socket failed,",,1,HAB-2102,LoiSuites Iguazu Hotel,3.6.0.0.709,0,1,192.168.128.216,,,192.168.128.216,520023,,0,0,0,,,,dynamic,0,1,Online,false,0,0,,0,0,0,autoconf,2.985074626865672,492400,313856,H320,351702010054,Ruckus H320 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.255.0,192.168.128.1,,,,1,0,0,1525282546,20412,1,0,0,0,0,0,0,0,0,2863161567,1149663710,20388279,8799730,0,0,0,0,0,0,0,0,6663,0,0,0,0
E8:1D:A8:38:92:50,1525352447,E8:1D:A8:38:92:50,,,,"-25.596687,-54.545629",AR,1525352451,2889,000cbc03-192a-4e34-894f-3181b97fdf87,Iguazu,ART+3,192.168.128.1,power cycle,1,1500,5,"init socket failed,",,2,HAB-2202,LoiSuites Iguazu Hotel,3.6.0.0.709,0,1,192.168.128.219,,,192.168.128.219,520026,,0,0,0,,,,dynamic,0,1,Online,false,0,0,,0,0,0,autoconf,0.9900990099009901,492400,312684,H320,361702009815,Ruckus H320 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.255.0,192.168.128.1,,,,1,0,0,1525243003,20412,1,0,0,0,0,0,0,0,0,817807147,277346056,23169841,11543900,0,0,0,0,0,0,0,0,6661,0,0,0,0
'''

#df_system=pd.read_csv(StringIO.StringIO(data_system),keep_default_na=False)
df_system=pd.read_csv(StringIO.StringIO(data_system))

data_system2='''apMac,sampleTime,ap,usbDeviceVersion,usbDeviceVID,usbDevicePID,gpsInfo,countryCode,timestamp,seqNumber,zone_id,zoneName,timeZone,gatewayIp,lastRebootReason,totalBootCount,mtuSize,rejoinCount,rejoinReason,oops,lossConnectBootCnt,deviceName,location,fwVersion,devSupportUsb,deviceIpMode,ip,ipv6,ipsecIp,apConnectedIp,uptime,mountState,currentTemperature,lifeMaxTemperature,lifeMinTemperature,dnatInfo,rksDpIp,rksDpIpOnly,ipType,isIpTypeChanged,managementVlan,apState,isConnectionTotalCountFlagged,totalConnectedClient,crashDump,altitudeUnit,altitudeValue,poeMode,poeModeSetting,ipv6Type,cpuPercentage,totalMemory,freeMemory,model,serialNumber,desc,numRadio,szConnCpIp,szConnCpIpv6,szConnDpIp,szConnDpIpv6,netmask,IpDnsSvr1,IpDnsSvr2,Ipv6DnsSvr1,Ipv6DnsSvr2,ApStatus,firstJoinTime,lastBootTime,lastConfSyncTime,freeStorage,ethPortStatus,ethStateChange,numRogues,numAuthClients,rxByteRate,txByteRate,rxErrorPkts,txErrorPkts,RxDropPkts,LanStatsRxBytes,LanStatsTxBytes,LanStatsRxPkts,LanStatsTxPkts,LanStatsRxErrorPkts,LanStatsTxErrorPkts,LanStatsRxBcastPkts,LanStatsTxBcastPkts,LanStatsRxMcastPkts,LanStatsTxMcastPkts,LanStatsRxUcastPkts,LanStatsTxUcastPkts,LanStatsRxDroppedPkts,LanStatsTxDroppedPkts,LanStatsRxByteRate,LanStatsTxByteRate,TxDropPkts
E8:1D:A8:22:A7:10,1525352429,E8:1D:A8:22:A7:10,1,,,"-34.564651,-58.463316",AR,1525352433,2876,9aafc307-7d05-43c2-aecc-a9993bbcedb5,Belgrano,ART+3,192.168.100.1,power cycle,1,1500,9,"init socket failed,http request failed,",,4,AP-PR-01-31,Belgrano Day school,3.6.0.0.709,0,1,192.168.101.153,,,192.168.101.153,517589,,0,0,0,,,,dynamic,0,1,Flagged,false,1,0,,0,0,0,autoconf,20.0,254280,128796,R310,371709006615,Ruckus R310 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.254.0,192.168.0.5,8.8.8.8,,,1,0,0,0,5432,1,0,0,0,0,0,4107,1782493,0,1714251472,1915655341,12138787,6339306,482,0,0,0,0,0,0,0,9,0,0,0,0
E8:1D:A8:17:DF:E0,1525352446,E8:1D:A8:17:DF:E0,,mundo,,"-25.596687,-54.545629",AR,1525352450,2889,000cbc03-192a-4e34-894f-3181b97fdf87,Iguazu,ART+3,192.168.128.1,power cycle,1,1500,5,"init socket failed,",,1,HAB-2102,LoiSuites Iguazu Hotel,3.6.0.0.709,0,1,192.168.128.216,,,192.168.128.216,520023,,0,0,0,,,,dynamic,0,1,Online,false,0,0,,0,0,0,autoconf,2.985074626865672,492400,313856,H320,351702010054,Ruckus H320 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.255.0,192.168.128.1,,,,1,0,0,1525282546,20412,1,0,0,0,0,0,0,0,0,2863161567,1149663710,20388279,8799730,0,0,0,0,0,0,0,0,6663,0,0,0,0
E8:1D:A8:38:92:50,1525352447,E8:1D:A8:38:92:50,,,,"-25.596687,-54.545629",AR,1525352451,2889,000cbc03-192a-4e34-894f-3181b97fdf87,Iguazu,ART+3,192.168.128.1,power cycle,1,1500,5,"init socket failed,",,2,HAB-2202,LoiSuites Iguazu Hotel,3.6.0.0.709,0,1,192.168.128.219,,,192.168.128.219,520026,,0,0,0,,,,dynamic,0,1,Online,false,0,0,,0,0,0,autoconf,0.9900990099009901,492400,312684,H320,361702009815,Ruckus H320 Multimedia Hotzone Wireless AP,2,35.190.145.155,,,,255.255.255.0,192.168.128.1,,,,1,0,0,1525243003,20412,1,0,0,0,0,0,0,0,0,817807147,277346056,23169841,11543900,0,0,0,0,0,0,0,0,6661,0,0,0,0
'''
#df_system2=pd.read_csv(StringIO.StringIO(data_system2),keep_default_na=False)
df_system2=pd.read_csv(StringIO.StringIO(data_system2),keep_default_na=False)

parquetfile='./tmp/system.parquet'

df_system.to_parquet(parquetfile, engine='fastparquet', append=False, compression='GZIP')
df_system2.to_parquet(parquetfile, engine='fastparquet', append=True, compression='GZIP')

pf = ParquetFile(parquetfile)
df = pf.to_pandas()

data_radio='''apMac,sampleTime,radioId,channel,mode,band,radioMode,txPower,phyError,channelBlacklist,noiseFloor,rxBytes,rxFrames,rxRadioBytes,rxRadioFrames,txBytes,txFrames,txRadioBytes,txRadioFrames,retry,drop,rxMulticast,txMulticast,total,busy,rx,tx,channelWidth,ap,latency,capacity,connectionFailure,connectionAuthFailureCount,connectionAssocFailureCount,connectionTotalCount,numOfChannelChange,isLatencyFlagged,isCapacityFlagged,isConnectionFailureFlagged,isAirtimeFlagged,isRadioEnabled,secondaryChannel,eirp,connectionTotalFailureCount,PowerMgmtEnable,MeshEnable,RxErrorPkts,TxErrorPkts,RxPktErrorRate,TxPktErrorRate,TxPktRetryRate,TxRetryBytes,RxDropBytes,TxDropBytes,RxDropPkts,TotalAssocTime,NumAuthClients,NumMaxClients,NumAuthReqs,NumAuthResps,NumAuthSuccess,NumAuthFail,AuthFailRate,NumAssocReq,NumAssocResp,NumReassocReq,NumReassocResp,NumAssocSuccess,NumAssocFail,NumAssocDeny,AssocSuccessRate,AssocFailRate,ResourceUtil,RxSignalPkts,TxSignalPkts,TotalSignalPkts,AntennaGain,BeaconPeriod,RtsThreshold,FragThreshold,RxWepFail,RxDecryptCrcError,RxMicError,Rssi
E8:1D:A8:22:A7:10,1525352429,0,1,11ng,2.4G,11ng,max,71269,,-97,107656377,428005,13762304789,53269422,998673145,3225821,12864559517,48209731,1782483,0,15405,0,30,1,28,1,0,E8:1D:A8:22:A7:10,0,14,25.0,964,85,2013,0,false,false,true,false,true,0,20,1286,1,0,4107,1782483,0,3,3,0,0,0,0,0,0,0,2065,2065,1309,0,0,435,414,504,503,909,22,0,97,2,0,0,0,0,0,100,0,0,0,0,0,10
E8:1D:A8:22:A7:10,1525352429,1,36,11ac,5G,11ac,max,991,,-100,47581193,145070,52679681,195570,326086526,1363340,2165186091,22231186,14095,0,12429,0,0,0,0,0,2,E8:1D:A8:22:A7:10,0,28,80.0,341,130,1047,0,false,false,true,false,true,0,23,540,1,0,0,0,0,0,0,0,0,0,0,0,1,1,1684,1684,1126,0,0,132,132,851,851,983,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,15
E8:1D:A8:17:DF:E0,1525352446,0,6,11ng,2.4G,11ng,max,79,,-96,1925918057,3998011,1258473004,4045747,5657787433,8107243,139180971,22317040,3119,0,117056,0,0,0,0,0,0,E8:1D:A8:17:DF:E0,0,42,0.0,2,29,220,7,false,false,false,false,true,0,18,33,1,0,0,0,0,0,0,0,0,0,0,0,0,0,217,217,217,0,0,85,85,131,127,212,4,0,98,1,0,0,0,0,2,100,0,0,0,0,0,14
E8:1D:A8:17:DF:E0,1525352446,1,40,11ac,5G,11ac,max,12,,-104,1764207007,3007282,2956399349,3069466,4320313114,6480105,2751871504,16755848,924,0,13259,0,0,0,0,0,0,E8:1D:A8:17:DF:E0,0,48,0.0,0,0,35,5,false,false,false,false,true,0,19,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,70,70,70,0,0,16,16,54,54,70,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,17
E8:1D:A8:38:92:50,1525352447,0,1,11ng,2.4G,11ng,max,39,,-96,2967113748,5266160,2135994810,5498060,8299306895,10735972,3019585195,25551398,46233,0,35537,0,0,0,0,0,0,E8:1D:A8:38:92:50,0,31,0.0,18,29,237,7,false,false,false,false,true,0,18,51,1,0,0,0,0,0,0,0,0,0,0,0,0,0,214,214,214,0,0,107,107,102,101,208,1,0,99,0,0,0,0,0,2,100,0,0,0,0,0,31
E8:1D:A8:38:92:50,1525352447,1,52,11ac,5G,11ac,max,3,,-105,3065408072,3848613,784020514,3950986,4370432446,8419844,2970109028,19227406,1183,0,53554,0,0,0,0,0,0,E8:1D:A8:38:92:50,0,58,0.0,6,2,85,4,false,false,false,false,true,0,20,8,1,0,0,0,0,0,0,0,0,0,0,0,0,0,117,117,117,0,0,49,49,64,64,113,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,23
'''

df_radio=pd.read_csv(StringIO.StringIO(data_radio),keep_default_na=False)

data_radio='''apMac,sampleTime,radioId,channel,mode,band,radioMode,txPower,phyError,channelBlacklist,noiseFloor,rxBytes,rxFrames,rxRadioBytes,rxRadioFrames,txBytes,txFrames,txRadioBytes,txRadioFrames,retry,drop,rxMulticast,txMulticast,total,busy,rx,tx,channelWidth,ap,latency,capacity,connectionFailure,connectionAuthFailureCount,connectionAssocFailureCount,connectionTotalCount,numOfChannelChange,isLatencyFlagged,isCapacityFlagged,isConnectionFailureFlagged,isAirtimeFlagged,isRadioEnabled,secondaryChannel,eirp,connectionTotalFailureCount,PowerMgmtEnable,MeshEnable,RxErrorPkts,TxErrorPkts,RxPktErrorRate,TxPktErrorRate,TxPktRetryRate,TxRetryBytes,RxDropBytes,TxDropBytes,RxDropPkts,TotalAssocTime,NumAuthClients,NumMaxClients,NumAuthReqs,NumAuthResps,NumAuthSuccess,NumAuthFail,AuthFailRate,NumAssocReq,NumAssocResp,NumReassocReq,NumReassocResp,NumAssocSuccess,NumAssocFail,NumAssocDeny,AssocSuccessRate,AssocFailRate,ResourceUtil,RxSignalPkts,TxSignalPkts,TotalSignalPkts,AntennaGain,BeaconPeriod,RtsThreshold,FragThreshold,RxWepFail,RxDecryptCrcError,RxMicError,Rssi
E8:1D:A8:22:A7:10,1525352429,0,1,11ng,2.4G,11ng,max,71269,,-97,107656377,428005,13762304789,53269422,998673145,3225821,12864559517,48209731,1782483,0,15405,0,30,1,28,1,0,E8:1D:A8:22:A7:10,0,14,25.0,964,85,2013,0,false,false,true,false,true,0,20,1286,1,0,4107,1782483,0,3,3,0,0,0,0,0,0,0,2065,2065,1309,0,0,435,414,504,503,909,22,0,97,2,0,0,0,0,0,100,0,0,0,0,0,10
E8:1D:A8:22:A7:10,1525352429,1,36,11ac,5G,11ac,max,991,,-100,47581193,145070,52679681,195570,326086526,1363340,2165186091,22231186,14095,0,12429,0,0,0,0,0,2,E8:1D:A8:22:A7:10,0,28,80.0,341,130,1047,0,false,false,true,false,true,0,23,540,1,0,0,0,0,0,0,0,0,0,0,0,1,1,1684,1684,1126,0,0,132,132,851,851,983,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,15
E8:1D:A8:17:DF:E0,1525352446,0,6,11ng,2.4G,11ng,max,79,,-96,1925918057,3998011,1258473004,4045747,5657787433,8107243,139180971,22317040,3119,0,117056,0,0,0,0,0,0,E8:1D:A8:17:DF:E0,0,42,0.0,2,29,220,7,false,false,false,false,true,0,18,33,1,0,0,0,0,0,0,0,0,0,0,0,0,0,217,217,217,0,0,85,85,131,127,212,4,0,98,1,0,0,0,0,2,100,0,0,0,0,0,14
E8:1D:A8:17:DF:E0,1525352446,1,40,11ac,5G,11ac,max,12,,-104,1764207007,3007282,2956399349,3069466,4320313114,6480105,2751871504,16755848,924,0,13259,0,0,0,0,0,0,E8:1D:A8:17:DF:E0,0,48,0.0,0,0,35,5,false,false,false,false,true,0,19,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,70,70,70,0,0,16,16,54,54,70,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,17
E8:1D:A8:38:92:50,1525352447,0,1,11ng,2.4G,11ng,max,39,,-96,2967113748,5266160,2135994810,5498060,8299306895,10735972,3019585195,25551398,46233,0,35537,0,0,0,0,0,0,E8:1D:A8:38:92:50,0,31,0.0,18,29,237,7,false,false,false,false,true,0,18,51,1,0,0,0,0,0,0,0,0,0,0,0,0,0,214,214,214,0,0,107,107,102,101,208,1,0,99,0,0,0,0,0,2,100,0,0,0,0,0,31
E8:1D:A8:38:92:50,1525352447,1,52,11ac,5G,11ac,max,3,,-105,3065408072,3848613,784020514,3950986,4370432446,8419844,2970109028,19227406,1183,0,53554,0,0,0,0,0,0,E8:1D:A8:38:92:50,0,58,0.0,6,2,85,4,false,false,false,false,true,0,20,8,1,0,0,0,0,0,0,0,0,0,0,0,0,0,117,117,117,0,0,49,49,64,64,113,0,0,100,0,0,0,0,0,3,100,0,0,0,0,0,23
'''

df_radio=pd.read_csv(StringIO.StringIO(data_radio),keep_default_na=False)



incldign this
[2018-05-30 06:29:39,264] INFO Processing file: home/uploads/2018050606_GWC2BCQK.zip from bucket: mediatel-push


