module pure_mongos

require (
	github.com/globalsign/mgo v0.0.0-20180615134936-113d3961e731
	github.com/go-stack/stack v1.8.0 // indirect
	go.mongodb.org/mongo-driver v1.0.0
	gopkg.in/mgo.v2 v2.0.0-20160801194620-b6121c6199b7
)

replace go.mongodb.org/mongo-driver v1.0.0 => github.com/mongodb/mongo-go-driver v1.0.0

replace gopkg.in/mgo.v2 v2.0.0-20160801194620-b6121c6199b7 => github.com/go-mgo/mgo v0.0.0-20160801194620-b6121c6199b7
