const test = require('tape');
const exec = require('child_process').exec ;

test('starting docker network..', (t) => {
  exec(`docker-compose -f ${__dirname}/docker-compose-testbed.yaml up -d`, (err, stdout, stderr) => {
    setTimeout(() => {
      t.pass('docker started');
      t.end(err);    
    }, 750);
  });
});
