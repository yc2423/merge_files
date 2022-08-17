import sys
import multiprocessing
import time
import uuid
import os

class solution:
	def __init__(self, input_dir, output_file_path):
		'''
		input_dir: The input directory name
		temp_dir:  A temp directory to store files that are generated during them merging process.
		output_file_path: The path of the final file
		file_list: A list of files that are waiting to be merged
		'''
		self.input_dir = input_dir
		self.temp_dir = None
		self.output_file_path = self.maybe_generate_dir(output_file_path)
		self.file_list = [] 

	def get_file_list(self, dir_path):
		'''
		Get a list of files that are under the dir_path
		'''
		res = []
		for path in os.listdir(dir_path):
			complete_path = os.path.join(dir_path, path)
			if os.path.isfile(complete_path):
				res.append(complete_path)

		return res

	def maybe_generate_dir(self, path):
		'''
		Check if the the directory with "path" exists. If not, create a directory.
		'''
		dir_path = os.path.dirname(path)
		if not os.path.exists(dir_path):
			print('[INFO]: The directory [' + dir_path + '] does not exist yet. Creating the directory.')
			os.makedirs(dir_path)
		else:
			print('[INFO]: The directory [' + dir_path + '] already exists.')

		return path

	def generate_process_input(self):
		'''
		Generate inputs of the function `merge_two_files`
		The input of `merge_two_files` is a tuple (file1, file2, output_file_path)
		EX:
		self.file_list = [file1, file2, file3, file4, file5]
		the result of the current function would be [(file1, file2, output_path), (file3, file4, output_path)]
		file5 will be kept for the next round of the merge process.
		'''
		res = []
		index = 0
		while (index+1) < len(self.file_list):
			res.append((self.file_list[index], self.file_list[index+1], self.temp_dir))
			index+=2
		return res

	def maybe_remove_files(self):
		'''
		The files in the self.file_list have be merged to new files, so we want to delete them
		If the count in self.file_list is odd, it means that the last file in self.file_list haven't been merged yet, and we should keep it.
		'''
		if self.file_list[0][0:len(self.input_dir)] == self.input_dir:
			return

		if len(self.file_list)%2 == 1:
			self.file_list.pop(-1)

		for file in self.file_list:
			if os.path.exists(file):
				os.remove(file)
			else:
				print("[Warning]: Trying to remove a non-exitng file: " + file)

	def manage_one_file(self, file):
		'''
		If there is only one file in the input directory, 
		we can remove blank lines and duplicate words, and print the content to the output file directly
		'''
		with open(file, 'r') as input_file, open(self.output_file_path, 'w+') as output_file:

			line = next(input_file, None)
			last_element = 0

			while line != None:
				print(line, last_element)
				if line != '\n' and line != last_element:
					last_element = line
					output_file.write(line)

				line = next(input_file, None)

		input_file.close()
		output_file.close()


	def merge_files(self):
		'''
		The main function in the class. 
		'''

		pool = multiprocessing.Pool(5)

		self.file_list = self.get_file_list(self.input_dir)

		if len(self.file_list) == 1:
			print("[INFO]: There is only one file in the input directory: "+ self.file_list[0])
			self.manage_one_file(self.file_list[0])

		else:
			self.temp_dir = self.maybe_generate_dir("temp_" + str(uuid.uuid4()) + '/')

			while len(self.file_list) > 1:
				print("[INFO]: " + str(len(self.file_list)) + " files waiting to be merged ...")
				inputs = self.generate_process_input()

				pool.map(merge_two_files, inputs)
				self.maybe_remove_files()
				self.file_list = self.get_file_list(self.temp_dir)

			os.rename(self.file_list[0], self.output_file_path)
			os.rmdir(self.temp_dir)

		print("[INFO]: The merging process is completed. The output file is generated: " + self.output_file_path)

def merge_two_files((file1, file2, temp_dir)):
	'''
	merge two files and put the result in the temp_dir
	We compare the two files line by line. 
	If a line in one file has a smaller lexicographical order, write that line to the output file, and move the iterator to the next line.
	'''
	
	output_file = temp_dir + str(uuid.uuid4()) + '.txt'
	with open(file1, 'r') as f1, open(file2, 'r') as f2, open(output_file, 'a+') as output:
		
		element1 = next(f1, None)
		element2 = next(f2, None)
		last_element = 0
		
		while element1 != None or element2 != None:
			if element1 in ["\n", last_element]:
				element1 = next(f1, None)
				continue
			if element2 in ["\n", last_element]:
				element2 = next(f2, None)
				continue

			if element1 == None:
				last_element = element2
				output.write(element2)
				element2 = next(f2, None)
			elif element2 == None:
				last_element = element1
				output.write(element1)
				element1 = next(f1, None)
			elif element1 < element2:
				last_element = element1
				output.write(element1)
				element1 = next(f1, None)
			else:
				last_element = element2
				output.write(element2)
				element2 = next(f2, None)


	f1.close()
	f2.close()
	output.close()

def main():
	if len(sys.argv) < 3:
		print("[Error]: Invalid input count. Please enter the input_dir and output_file_path.")
		return 
	
	input_dir, output_file_path = sys.argv[1], sys.argv[2]

	solutions = solution(input_dir, output_file_path)
	solutions.merge_files()

if __name__ == '__main__':	
	main()

