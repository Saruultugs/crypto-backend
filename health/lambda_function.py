#!/Users/asif/anaconda2/bin/python

config = {
  'function_name': 'health',
  'fresh_build': True,
  'clear_build': True
}
log = ['Running ' + config['function_name']]

def lambda_handler(event, context):
  from firebase import firebase
  import time
  fb = firebase.FirebaseApplication('https://crypto-trading-953aa.firebaseio.com/', None)
  rds = {}
  ts = str(int(time.time()))

  def getMetrics():
    # rds['prev_state'] = fb.get('rds', None)
    rds['availability'] = 0.997
    rds['record_count_actual'] = 90
    rds['record_count_expected'] = 100
    log.append('Current metrics: ' + str(rds))

  def generateAnalysis():
    rds['record_count_score'] = float(rds['record_count_actual']) / rds['record_count_expected']
    rds['scores'] = [rds['availability'], rds['record_count_score']]
    rds[' overall_health'] = sum(rds['scores']) / len(rds['scores'])

  def writeToFirebase():
    log.append('RDS: ' + str(rds))
    for key, value in rds.iteritems():
      if not key == 'prev_state':
        fb.put('rds/' + ts, key, value)

  getMetrics()
  generateAnalysis()
  writeToFirebase()
  return log

if __name__ == '__main__':
  import os
  os.system('clear')
  print 'Building and deploying package to Lambda: ' + config['function_name']
  if config['fresh_build']:
    os.system('ls | grep -v lambda_function.py | xargs rm -r')
  if not os.path.exists('requests'):
    os.system('pip install requests -t .')
  if not os.path.exists('firebase'):
    os.system('pip install python-firebase -t .')
  # if not os.path.exists('pymysql'):
  #   os.system('pip install pymysql -t .')
  os.system('zip -rq deployment-package.zip ./*')
  os.system('aws lambda update-function-code --function-name ' + config['function_name'] + ' --zip-file fileb://./deployment-package.zip')
  if config['clear_build']:
    os.system('ls | grep -v lambda_function.py | xargs rm -r')
  print 'Running function'
  os.system('aws lambda invoke --function-name ' + config['function_name'] + ' lambda_return.txt')
  os.system('clear')
  print 'Lambda return:'
  os.system('cat lambda_return.txt')
  os.system('rm lambda_return.txt')
  print
