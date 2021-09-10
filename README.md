# provider_channel_xband
Retrieve stored data into channel

usage:
 
 Usage of channel_xband.exe:
  -all Bool
        Send all file in data storage (default true)
  -channel int
        Retrieving channel type (default 3)
  -dir string
        Directory of data storage (default "xbanddata")
  -startDate string
        Specify Start Date (default "01-01")
  -startTime string
        Specify Start Time (default "00:00")
  -endDate string
        Specify End Date (default "12-31")
  -endTime string
        Specify End Time (default "24:00")
  -local string
        Specify Local Synerex Server
  -nodesrv string
        Node ID Server (default "127.0.0.1:9990")
  -completion Bool
        Completing data between xband data (default false)
  -miniGapTime int
        Minimum time difference. (seconds) (default 60)
  -compdiv int
        Number of splits (default 6)
  -skip int
        Skip lines (default 0)
  -speed float
        Speed of sending packets (default 1.0) if speed is minus, just sleep for each packet.
