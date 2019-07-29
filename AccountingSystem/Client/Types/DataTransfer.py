#!/usr/bin/env python
from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class DataTransfer(BaseAccountingType):

  def __init__(self):
    BaseAccountingType.__init__(self)

    self.definitionKeyFields = [
                                ( 'User', 'VARCHAR(32)'),
                                ( 'Source', 'VARCHAR(32)'),
                                ( 'Destination', 'VARCHAR(32)'),
                                ( 'Protocol', 'VARCHAR(32)'),
                                ( 'FinalStatus', 'VARCHAR(32)')
                               ]
    self.definitionAccountingFields = [ 
                                        ( 'TransferSize', 'BIGINT UNSIGNED' ),
                                        ( 'TransferTime', 'FLOAT' ),
                                        ( 'TransferOK', 'INT UNSIGNED' ),
                                        ( 'TransferTotal', 'INT UNSIGNED' )
                                      ]

    self.checkType()
