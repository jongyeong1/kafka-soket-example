//소켓 전역설정 프로듀서랑 컨슈머 같이 구현
const kafka         = require('kafka-node');
const Consumer      = kafka.Consumer;
const ClientKafka   = new kafka.Client(process.env.KAFKA_SERVER_URL);
const Producer      = new kafka.HighLevelProducer(ClientKafka);

// 컨슈며 정의 - 토픽 지정
const _getConsumer = () => {
    let topic = 'socketTopicTest';
    return new Consumer(ClientKafka, [{ topic: topic, partition: 0 }], {autoCommit: true});
};

// 소켓을 통한 컨슈머 커넥팅
const _openChatConnection = (io, consumer) => {
    io.on('connection', (socket) => {
        //Listen socket.io frontend
        socket.on('subscribe_front', (data) => {
            console.log(data);
        });
        //Listen kafka producer
        var list = '';
        consumer.on('message', function(data) {
            list += data;
        });
        consumer.on('end',function(){
            socket.emit('send_task', JSON.parse(list.value));  // end 안해주면 형식끝나기전마다 읽어서 에러 나옴
        });
    });
};

//토픽 가지고 오기
const _getTopic = () => {
    return new Promise((resolve, reject) => {
        Producer.on('ready', function(errProducer, dataProducer) {
            Producer.createTopics(['socketTopicTest'], true, function (err, data) {
                if (!err) {
                    resolve(true);
                } else {
                    reject();
                }
            });
        })
    });
};


module.exports = {
    init: (io) => {
        try {
            _getTopic().then(topicIsCreated => {
                let consumer = _getConsumer();
                _openChatConnection(io, consumer);
            }).then(err => {
                console.log(err);
            });            
        } catch (e) {
            console.log(e);
        }
    }
};
