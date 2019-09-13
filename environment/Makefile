.PHONY: clean-image, build, run-image

clean-image:
	sudo docker rmi -f $$(sudo docker images | head -n2 | tail -n1 | awk '{ print $$3 }')

build:
	sudo docker build -t phd-health-mobility-build:0.0.1 .

run-image:
	sudo docker run -i -t $$(docker images | head -n2 | tail -n1 | awk '{ print $$3 }') /bin/bash
