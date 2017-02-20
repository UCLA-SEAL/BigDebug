from random import randint
import argparse

# Args as follows : filen

parser = argparse.ArgumentParser(description=' College students data generator ')
parser.add_argument('-o', action="store", dest="outputfile", help='provide output file name', default='studentData.txt')
parser.add_argument('-n', action="store", dest="num", type=int, help="nubmer of rows in the dataset", default=100000)
results = parser.parse_args()

f = open(results.outputfile, "w")
alphabetList = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
                'v', 'w', 'x', 'y', 'z']
genderList = ["male", "female"]
gradeList = ["0", "1", "2", "3"]
majorList = ["English", "Mathematics", "ComputerScience", "ElectricalEngineering", "Business", "Economics", "Biology",
             "Law", "PoliticalScience", "IndustrialEngineering"]
i = 0
while i != results.num:
    i = i + 1
    firstNameLength = randint(1, 10)
    lastNameLength = randint(1, 15)
    firstName = ''
    lastName = ''
    for num in range(0, firstNameLength):
        firstName = firstName + alphabetList[randint(0, 25)]
    for num in range(0, lastNameLength):
        lastName = lastName + alphabetList[randint(0, 25)]
    gender = genderList[randint(0, 1)]
    grade = gradeList[randint(0, 3)]
    major = majorList[randint(0, 9)]
    fault1 = randint(0, 10000000)
    fault2 = randint(0, 10000000)
    if grade == "0":
        if fault1 <= 100:  # fault version 1000/10000000
            age = randint(18000, 19000)
        else:  # correct version
            age = randint(18, 19)
    elif grade == "1":
        age = randint(20, 21)
    elif grade == "2":
        age = randint(22, 23)
    else:
        age = randint(24, 25)
    if major == "ComputerScience":
        if fault2 <= 500:  # fault version 5000/1000000
            age = 99999999
    fault3 = randint(0, 5000000)
    if fault3 <= 500:  # fault version 5000/5000000
        f.write(firstName + " " + lastName + " " + gender + " " + grade + " " + str(age) + " " + major + "\n")
    else:
        f.write(firstName + " " + lastName + " " + gender + " " + str(age) + " " + grade + " " + major + "\n")
f.close()