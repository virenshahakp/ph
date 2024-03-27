import argparse
import sqlparse
import sys

input_file = None
output_file = None

parser = argparse.ArgumentParser(description='format SQL')
parser.add_argument('--input', default=None, help='input file')
parser.add_argument('--output', default=None, help='output file')
args = parser.parse_args()

if args.input:
    input_file = open(args.input, 'r')
else:
    input_file = sys.stdin

# read input before opening output in case the two paths are the same
unformatted = input_file.read()

formatted = sqlparse.format(unformatted, keyword_case='upper', comma_first=True)

if args.output:
    output_file = open(args.output, 'w')
else:
    output_file = sys.stdout

output_file.write(formatted)
