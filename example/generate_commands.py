import os, stat


PARTICIPANTS = ['localhost:9876',
                'localhost:9877',
                'localhost:9878']
                # 'localhost:9879',
                # 'localhost:9880']

TMPL = '#!/usr/local/bin/bash -x\npython helloworld.py logs/{n}.sqlite {host} {port} {peers}'

for n, p in enumerate(PARTICIPANTS):
    host, _, port = p.partition(':')
    peers = [peer for peer in PARTICIPANTS if peer != p]
    cmd = TMPL.format(n=n, host=host, port=port,
                      peers=' '.join(peers))
    fn = 'peer_{n}'.format(n=n)
    with open(fn, 'w') as f:
        f.write(cmd + '\n')
    st = os.stat(fn)
    os.chmod(fn, st.st_mode | stat.S_IEXEC)
