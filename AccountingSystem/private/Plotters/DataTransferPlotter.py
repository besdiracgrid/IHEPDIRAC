from DIRAC import S_OK, S_ERROR
from IHEPDIRAC.AccountingSystem.Client.Types.DataTransfer import DataTransfer
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

class DataTransferPlotter( BaseReporter ):
  _typeName = "DataTransfer"
  _typeKeyFields = [ df[0] for df in DataTransfer().definitionKeyFields]

  def _reportDataTransfered( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'TransferSize' ]
                   )
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    for key in dataDict:
      dataDict[ key ] = int( dataDict[ key ] )
    return S_OK( { 'data' : dataDict  } )

  def _plotDataTransfered( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Total data transfered by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'bytes',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

  def _reportThroughput( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength', 'TransferSize' ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict, maxValue = self._divideByFactor( dataDict, granularity )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit( dataDict,
                                                                                  self._getAccumulationMaxValue( dataDict ),
                                                                                  "bytes" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotThroughput( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Throughput by %s' % reportRequest[ 'grouping' ] ,
                 'ylabel' : plotInfo[ 'unit' ],
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  # Generate Transfer Data Accumulate
  def _reportDataSizeBin( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'TransferSize'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    dataDict = self._fillWithZero( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    #dataDict = self._accumulate( granularity, reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], dataDict )
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit( dataDict,
                                                                              self._getAccumulationMaxValue( dataDict ),
                                                                              "bytes" )
    return S_OK( { 'data' : baseDataDict, 'graphDataDict' : graphDataDict,
                   'granularity' : granularity, 'unit' : unitName } )

  def _plotDataSizeBin( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Transfered data by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'ylabel' : plotInfo[ 'unit' ],
                 'sort_labels' : 'last_value' }
    return self._generateTimedStackedBarPlot( filename, plotInfo[ 'graphDataDict' ], metadata )

  # Quality
  def _reportQuality( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", %s, %s, SUM(%s), SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'startTime', 'bucketLength',
                                    'TransferOK', 'TransferTotal'
                                   ]
                   )
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                { 'checkNone' : True,
                                  'convertToGranularity' : 'sum',
                                  'calculateProportionalGauges' : False,
                                  'consolidationFunction' : self._efficiencyConsolidation } )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    self.stripDataField( dataDict, 0 )
    if len( dataDict ) > 1:
      #Get the total for the plot
      selectFields = ( "'Total', %s, %s, SUM(%s),SUM(%s)",
                       [ 'startTime', 'bucketLength',
                         'TransferOK', 'TransferTotal'
                       ]
                     )
      retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                  reportRequest[ 'endTime' ],
                                  selectFields,
                                  reportRequest[ 'condDict' ],
                                  reportRequest[ 'groupingFields' ],
                                  { 'checkNone' : True,
                                    'convertToGranularity' : 'sum',
                                    'calculateProportionalGauges' : False,
                                    'consolidationFunction' : self._efficiencyConsolidation } )
      if not retVal[ 'OK' ]:
        return retVal
      totalDict = retVal[ 'Value' ][0]
      self.stripDataField( totalDict, 0 )
      for key in totalDict:
        dataDict[ key ] = totalDict[ key ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotQuality( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Transfer quality by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ] }
    return self._generateQualityPlot( filename, plotInfo[ 'data' ], metadata )
