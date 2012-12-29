ARYA
====

This project in development. README will be updated.

Why do we need this?
--------------------

Modify python dictionaries or object instances on the fly? What does it mean? Why do we need this?

Ok, guys, just give me a chance to explain. After that you will probably say: "this is a very rare use case, I really can't imagine where can I use this piece of code". And you will be right. But in this 
case just try to remember about this project - and maybe some day...


So, imagine that at some point of your program you have a dictionary with known keys, for example Message in chat application (all I write about dictionary here can be applied to object instances too). Something like this:

```python
message = {
	'username': 'fzambia',
	'text': 'please, fork me on Github!',
	'created': '21:40:39'
}
```

Generally you do not need to make any modifications with this message - just save it into database and broadcast to all who interested. But what if one of your clients complained that after 21:00 and before 09:00 he don't want to receive messages which contain Github word. Yeah, it's very naive and stupid example, but this is just an example. You can provide an interface for task like this. But then another user will complain that he wants to replace "Github" word with "Bitbucket" in all messages from "fzambia". Here you can begin to think about something generic.

Lets write some code and change that dict accorting to our complaints:

```python
from arya import Pipe

message = {
	'username': 'fzambia',
	'text': 'please, fork me on Github!',
	'created': '21:40:39'
}

p = Pipe()
pipe = {
	"match": {
		"created": {
			"mode": "or",
			"conditions": [("lte", "09:00:00"), ("gte", "21:00:00")],
			"type": "time",
			"format": "%H:%M:%S"
		}
	},
	"alter": {
		"drop": "ALL"
	}
}
# this is for our first complaint
altered_message = p.apply(message, pipe)
assert altered_message is None

pipe = {
	"match": {
		"username": {
			"conditions": [("exact", "fzambia")],
		}
	},
	"alter": {
		"replace": {
			"text": {
				"value": "Github",
				"replacement": "Bitbucket"
			}
		}
	}
}
# this is for our second complaint
altered_message = p.apply(message, pipe)
assert altered_message['text'] == 'please, fork me on Bitbucket!'

```

  