.PHONY: format clean

FMT = python3 format.py

DIRS := models
SQL_FILES := $(shell find $(DIRS) -type f -name '*.sql')
FORMATTED_SQL_FILES := $(patsubst %.sql,%.sql.fmt,$(SQL_FILES))

%.sql.fmt: %.sql
	$(FMT) --input $< --output $@
	mv $@ $<

format: $(FORMATTED_SQL_FILES)

clean:
	rm -f ${FORMATTED_SQL_FILES}
