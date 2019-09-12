''' CLOUDAccessTest

A test class to test the access to clouds.

'''

from urlparse import urlparse

from DIRAC import S_OK, S_ERROR, gConfig


class CLOUDAccessTest:
  ''' CLOUDAccessTest
  '''

  def _getAccessParams( self, element ):
    '''
      get the access host and port for the specified cloud.
    '''

    _basePath = 'Resources/Sites'
    _searchKey = ( 'ex_force_auth_url', 'EndpointUrl' )

    cloud = gConfig.getSections( '%s/CLOUD/%s/Cloud' % ( _basePath, element ) )
    if not cloud[ 'OK' ]:
      return cloud
    cloud = cloud[ 'Value' ][ 0 ]

    for key in _searchKey:
      url = gConfig.getValue( '%s/CLOUD/%s/Cloud/%s/%s' % ( _basePath, element, cloud, key ) )
      if url:
        o = urlparse(url)
        if o.port:
          port = o.port
        else:
          port = 443 if o.scheme == 'https' else 80
        return S_OK((o.hostname, port))

    return S_ERROR('%s is not a vaild CLOUD.' % element)
