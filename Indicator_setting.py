indparams = {
    'ma' : {
        'period' : [5,10,20]
    },
    'rsi' : {
        'period' : [5,10,20]
    },
}

import json
with open('Indicator_setting.json','w') as f:
    json.dump(indparams,f)
