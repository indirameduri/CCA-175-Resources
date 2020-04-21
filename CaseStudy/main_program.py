import pyfiglet
import pprint
import random
import requests
from termcolor import colored
from colorama import Fore, Back, Style, init
import time
import os
from kafkaTospark import *
from mariaMongo import *

def main():
    
    init()
    text = "KAFKA - SPARK CASE STUDY"
    ascii_text = pyfiglet.figlet_format(text,font='bubble')
    
    print(Fore.RED + ascii_text)
    entry=0 
    while(entry!='4'):
        print('KAFKA - SPARK CASE STUDY')
        print('------------------------')
        
        print('a) Credit Card Management')
        print('b) Health Insurance Management')
        print('c) Kafka->Spark->Mongo Data Transfer')
        print('d) Quit')
        entry=input('\nSelect Options: ').lower()
        
        if entry=='a':
            print('\nOption 1 will transfer structured data from MariaDB to Spark.\nTransformations are implemented on this data and then loaded into MongoDB for future Analysis\nPlease watch for sample data and also the operations happening in this process\n ')
            mariaToMongo()
            print('Data transferred from MariaDB to MongoDb after transformations in Spark!!')
        elif entry == 'b':
            print('\nOption 2 will capture website data and transfer to Kafka.\n')
            ans = 'y'
            if (ans == 'y' or ans == 'Y'):
                input('If Zookeeper and Kafka Servers are up and running......enter(y/Y) to continue: ')
                URLtoKafka()
            else:
                print('Invalid Input!')
                continue
        elif entry == 'c':
            print('\n Because of Java Heap Size issues we\'ll do step wise transfer from kafka -> Spark -> Mongo\n')
            opt='0'
            

            while(opt in ['1','2','3','4','5','6','7','8','9']):
                loaded=0
                print('\n1.BenefitsCostSharing1\n2.BenefitsCostSharing2\n3.BenefitsCostSharing3\n4.BenefitsCostSharing\n5.Network\n6.Service Area\n7.Insurance\n8.Plan Attribute\n9.Quit\n')
                opt = input('Enter 1-9: ')
                if opt == '1':
                    if loaded == 0:
                        BenefitsCostSharing1()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '2':
                    if loaded == 0:
                        BenefitsCostSharing2()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '3':
                    if loaded == 0:
                        BenefitsCostSharing3()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '4':
                    if loaded ==0:
                        BenefitsCostSharing4()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '5':
                    if loaded ==0:
                        Network()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '6':
                    if loaded==0:
                        Insurance()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '7':
                    if loaded == 0:
                        ServiceArea()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '8':
                    if loaded == 0:
                        Plan_Attribute()
                        loaded=1
                    else:
                        print('Already trasferred!')
                elif opt == '9':
                    break
                else:
                    print('Select 1-8 only!')
                
            else:
                print('Invalid Input!')
                continue
        elif entry =='d':
                print('Goodbye!')
                break
        else:
            print('Invalid Input')
            continue
        
if __name__=='__main__':
    main()