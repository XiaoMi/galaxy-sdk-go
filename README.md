# Installing

***

Install your specific service package with the following go get command. 
You can install the entire SDK by installing the root package:

`$ go get github.com/XiaoMi/galaxy-sdk-go`

# Configuring Credential

***

Before using the SDK, ensure that you've configured credential. 
You can get the necessary credential by registering on [http://dev.xiaomi.com/]().
To configure credential, you may use codes like:

```
appKey := "MY-APP-KEY"
appSecret := "MY-SECRET-KEY"
userType := auth.UserType_APP_SECRET
cred := auth.Credential{&userType, &appKey, thrift.StringPtr(appSecret)}
```
# Usage

***

To use SDK, you can import like:

```
import (
	"github.com/XiaoMi/galaxy-sdk-go/sds/auth"
	"github.com/XiaoMi/galaxy-sdk-go/sds/common"
	"github.com/XiaoMi/galaxy-sdk-go/sds/client"
	"github.com/XiaoMi/galaxy-sdk-go/sds/table"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
)

```

We have an example in examples/basic.go, users can run this example after
credential configured:

```
$ cd examples
$ go get
$ go run basic.go
```

