package iotflow

type deviceState uint32

const (
	nmDeviceStateUnknown      deviceState = 0
	nmDeviceStateUnmanaged    deviceState = 10
	nmDeviceStateUnavailable  deviceState = 20
	nmDeviceStateDisconnected deviceState = 30
	nmDeviceStatePrepare      deviceState = 40
	nmDeviceStateConfig       deviceState = 50
	nmDeviceStateNeedAuth     deviceState = 60
	nmDeviceStateIPConfig     deviceState = 70
	nmDeviceStateIPCheck      deviceState = 80
	nmDeviceStateSecondaries  deviceState = 90
	nmDeviceStateActivated    deviceState = 100
	nmDeviceStateDeactivating deviceState = 110
	nmDeviceStateFailed       deviceState = 120
)
