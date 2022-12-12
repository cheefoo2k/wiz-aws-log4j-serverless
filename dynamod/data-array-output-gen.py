#Usage: python data-array-output-gen.py --num_of_lines 1 --load_type dynamodb
#Usage: python data-array-output-gen.py --num_of_lines 2 --load_type redshift
#To generate 600,000 lines , pass 600,000 to arg num_of_lines

from faker import Faker
import random
import argparse
import boto3
import json
from  dynamodbwriter import DynamoWriter

NUM_TO_GENERATE = 1000
fake = Faker()
parameter_array = ['aaaccreq ',	'aaaop ',	'aab25 ',	'aab25req ',	'aabap ',	'aacaidnpr ',	'aacaiuppr ',	'aacjc ',	'aaengvib ',	'aaeothrs ',	'aafadeceqid ',	'aafadecswid ',	'aafanang ',	'aafanwgt ',	'aaferrdeb ',	'aaffdp ',	'aafltwd1 ',	'aafltwd10 ',	'aafltwd11 ',	'aafltwd14 ',	'aafltwd2 ',	'aafltwd3 ',	'aafltwd4 ',	'aafltwd5 ',	'aafltwd6 ',	'aafltwd7 ',	'aafltwd8 ',	'aafltwd9 ',	'aaflxphm1 ',	'aaflxphm2 ',	'aaflxphm3 ',	'aaflxphm4 ',	'aafmuft ',	'aafocreq ',	'aahpcbld ',	'aahpcsva ',	'aahpcsvareq ',	'aaitt ',	'        aaittal ',	'aaittrl ',	'aalpcsva ',	'aalpcsvareq ',	'aalptang ',	'aalptwgt ',	'aan1 ',	'aan1clbref ',	'aan1contref ',	'aan1idlref ',	'aan1ind ',	'aan1maxav ',	'aan1mcl ',	'aan1modenum ',	'aan1mtoref ',	'aan1ph ',	'aan1phmu ',	'aan1phr ',	'aan1rl ',	'aan1set ',	'aan1tlaset ',	'aan1toref ',	'aan1torefwf ',	'aan1vib ',	'aan1vibf ',	'aan2 ',	'aan2dot ',	'aan2phmu ',	'aan2rl ',	'aan2vib ',	'aan2vibr ',	'aancfan ',	'aanf ',	'aanferrdeb ',	'aanfph ',	'aanfvib ',	'aanfvibr ',	'aansa ',	'aaodmbin1 ',	'aaodmbin2 ',	'aaodmbin3 ',	'aaodmbin4 ',	'aaodmbin5 ',	'aaodmbin6 ',	'aaodmbin7 ',	'aaodmbin8 ',	'aaodmrms ',	'aaofdp ',	'aaoilq ',	'aap2 ',	'aap25 ',	'aap2eng ',	'aapam ',	'aapambe ',	'aapb ',	'aapecsh ',	'aapecsl ',	'aapoilac ',	'aaratcrc ',	'aarevl ',	'aarevr ',	'aastawd1 ',	'aastawd10 ',	'aastawd11 ',	'aastawd12 ',	'aastawd13 ',	'aastawd15 ',	'aastawd16 ',	'aastawd2 ',	'aastawd3 ',	'aastawd4 ',	'aastawd5 ',	'aastawd6 ',	'aastawd7 ',	'aastawd8 ',	'aastawd9 ',	'aat25 ',	'aat2e ',	'aat3 ',	'aat5 ',	'aatcai ',	'aatecsh ',	'aatecsl ',	'aateeci ',	'aatflexfbk ',	'aatla ',	'aatlanotrm ',	'aatoilac ',	'aatphmul ',	'aatt2sel ',	'aavib1rms ',	'aavib2rms ',	'aavorv ',	'aavorvreq ',	'aawf ',	'aawffmv ',	'aawfreq ',	'aaacs_pack_mass_flow ',	'aaacs_pack_mass_flow_intermediate_target ',	'aaacs_pack_mass_flow_target ',	'aoa ',	'altitude__gps_ ',	'altitude_rate ',	'angle_of_slideslip ',	'aabas_status_word ',	'aableed_status ',	'body_lateral_acceleration ',	'xacc ',	'body_normal_acceleration ',	'body_pitch_rate ',	'body_roll_rate ',	'body_yaw_rate ',	'aabrake_system_status ',	'brkpdl ',	'brkpdr ',	'cas ',	'cobrkpdl ',	'cobrkpdr ',	'aacross_wind__fmc_ ',	'distance_to_destination__master_ ',	'flaps ',	'gw ',	'aahead_wind__fmc_ ',	'aahigh_lift_configuration_word ',	'aaiams_bleed_data ',	'aaiccp_cont_ign_eng_start_park_brake_door ',	'aaiccp_ovrhd_7_bleed_anti_ice ',	'mach ',	'aaoelt ',	'pitch ',	'present_position_latitude__gps_ ',	'present_position_latitude_fine__gps_ ',	'present_position_longitude__gps_ ',	'present_position_longitude_fine__gps_ ',	'alt ',	'ralt ',	'roll ',	'slat ',	'static_air_temperature ',	'aatat ',	'abvalves_position_feedback_2 ',	'aawd ',	'woffw ',	'aaws ',	'aill ',	'ailr ',	'aural_alert_number_96 ',	'center_wing_tank_fuel_quantity__lbs ',	'cg ',	'consolidated_gear_data ',	'consolidated_wow_data_ ',	'cpcs_cabin_differential_pressure ',	'eicas_avionics_disc_wd_2_1 ',	'fbw_l_elev_posn_syn ',	'fbw_l_mfs1_posn_syn ',	'fbw_l_mfs2_posn_syn ',	'fbw_l_mfs3_posn_syn ',	'fbw_l_mfs4_posn_syn ',	'fbw_pitch_trim_posn_trm ',	'fbw_r_elev_posn_syn ',	'fbw_r_mfs1_posn_syn ',	'fbw_r_mfs2_posn_syn ',	'fbw_r_mfs3_posn_syn ',	'fbw_r_mfs4_posn_syn ',	'fbw_sel_copilot_pitch_ssc_pos ',	'fbw_selected_copilot_roll_stick_command ',	
'fbw_sel_pilot_pitch_ssc_posn ',	'fbw_sel_pilot_roll_ssc_posn ',	'fbw_sel_rud_pedal_posn ',	'inertial_vertical_speed ',	'left_wing_tank_fuel_quantity__lbs ',	'left_outboard_wheel_speed ',	'right_wing_tank_fuel_quantity__lbs ',	'right_outboard_wheel_speed ',	'rudd ',	'sfecu_oms_19_33 ',	'sfecu_oms_20_33 ',	'sfecu_oms_21_33 ',	'sfecu_oms_22_33 ',	'thdg ',	'total_fuel_quantity__lbs']

def create_rows_faker():
    array_of_readings = [None] * NUM_TO_GENERATE 
    faker_readings = [fake.date(),str(fake.pyfloat()),str(fake.date())]
    for _ in range(NUM_TO_GENERATE):
        #reading = fake.random_elements(elements=(fake.date(),  fake.pyfloat(), fake.date()), unique=False, length=3)
        array_of_readings[_] = faker_readings
    return array_of_readings


def create_random_guid(num_of_lines):
    array_of_guids = [None] * num_of_lines
    array_of_guids_parameter = [None] * num_of_lines * len(parameter_array)

    faker_guids = fake.uuid4()
    for _ in range(num_of_lines):
        array_of_guids[_] = faker_guids

    for _ in range(num_of_lines):
        guids_keeper = array_of_guids[_]
        for i in range(len(parameter_array)):
            print (guids_keeper+'_'+parameter_array[i]+'\n')
            array_of_guids_parameter[i] = guids_keeper+'_'+parameter_array[i]
    
    return array_of_guids_parameter

def simulate_load(num_of_lines):
    publisher_readings = [None] * int(num_of_lines)*len(parameter_array)
    for _ in range(int(num_of_lines)*len(parameter_array)):
        output = create_rows_faker()
        publisher_readings[_] = output
    guids = create_random_guid(int(num_of_lines))
    return publisher_readings,guids



def construct_dynamodb_item_to_save(guids,publisher_readings,num_of_lines):
    print("In DDB COnstruct")
    f = open("dynamo_simulation_results.txt", "a")
    for _ in range(len(guids)):
        request = {"guid_parameter" : guids[_], "pub_value" : publisher_readings[_]}
        #print(request)
        #ddb_json_request = json.dumps(request).replace("'",'"')
        #f.write(str(ddb_json_request))
        dynamodb = boto3.resource('dynamodb')
        table=dynamodb.Table('publisher_data')
        #save to dynamodb
        writer = DynamoWriter(table)
        writer.write_to_dynamo_table(table,request)
        #writer.write_to_dynamo_table(table,ddb_json_request)

def construct_redshift_item_to_save(guids,publisher_readings,num_of_lines):
    print("In RedShift COnstruct")
    request = ''
    f = open("simulation_results.txt", "a")
    for _ in range(len(guids)):
        request = {"guid_parameter" : guids[_], "pub_value" : publisher_readings[_]}
        print(str(request)+'\n')
        f.write(str(request)+'\n')
    
   
    return request  

def main(num_of_lines,load_type):
    
    if(load_type=='dynamodb'):
        publisher_readings, guids = simulate_load(num_of_lines)
        construct_dynamodb_item_to_save(guids,publisher_readings,num_of_lines)
    elif(load_type=='redshift'):
        publisher_readings, guids = simulate_load(num_of_lines)
        construct_redshift_item_to_save(guids,publisher_readings,num_of_lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate Publisher data sample')
    parser.add_argument('--num_of_lines', metavar='path', required=True,
                        help='the path to publisher lines')
    parser.add_argument('--load_type', metavar='path', required=True,
                        help='dynamodb or redshift')
    args = parser.parse_args()
    main(num_of_lines=args.num_of_lines,load_type=args.load_type)








