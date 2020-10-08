import psycopg2
import pandas as pd
import configparser
import boto3
import json
from botocore.exceptions import ClientError
from sql_queries import copy_table_queries, insert_table_queries, create_table_queries, drop_table_queries, validation_queries
from time import sleep, time

def create_clients(config):
    '''
    Creates the aws objects to access those cloud functions

        Parameters:
            config: reference to the configuration file

        Returns:
            ec2: reference to the resource
            iam: reference to the client
            redshift: reference to the client
    '''

    KEY     = config.get('AWS','KEY')
    SECRET  = config.get('AWS','SECRET')

    ec2 = boto3.resource('ec2', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    # s3 = boto3.resource('s3', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET) # not needed for this project
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    return ec2, iam, redshift

def setup_iam_role(iam, config):
    '''
    Creates the IAM role

        Parameters:
            iam: reference to the client object
            config: reference to the configuration file

        Returns:
            roleArn: 
    '''

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # create the iam role
    try:
        print("\nSetting up IAM Role...") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except ClientError as ce:
        if "EntityAlreadyExistsException" in str(type(ce)):
            pass # this is an expected exception
        else:
            print ("Unexpected ClientError exception occurred....")
            print (ce)
    except Exception as e:
        print ("Unexpected exception occurred....")
        print(e)


    # attach policy
    res = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    if res != 200:
        print ("Error occured attaching policy. http response code =", res)


    # get the role arn
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print("  Role arn is ", roleArn)

    return roleArn

def setup_redshift_cluster(redshift, config, roleArn): 
    '''
    Creates a temporary redshift cluster

        Parameters:
            redshift: reference to the client object
            config: reference to the configuration file
            roleArn: role arn 

        Returns:
            nothing
    '''

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    # DWH_IAM_ROLE_ARN       = config.get("DWH", "DWH_ROLE_ARN")

    # Create a temporary Redshift cluster
    def createRedshiftCluster():
        try:
            response = redshift.create_cluster(        
                #HW
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
            )
            print ("\nRedshift cluster creation submitted. This may take a couple of minutes...")
        except Exception as e:
            print(e)

    if not len(redshift.describe_clusters()['Clusters']):
        createRedshiftCluster()
    else:
        print ("Redshift cluster already exists.")

    # See the cluster status
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    # script assumes no other clusters exist, as it grabs the first one returned from the describe_clusters function
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    # wait for redshift build to complete
    t = time()
    x = 400  # time out after x seconds
    while x > 0:
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if myClusterProps['ClusterStatus'] == 'available':
                print ("  Redshift cluster has been created.   (took {:0.0f} secs).".format(time() - t))
                break
            else:
                if x % 10 == 0:
                    print ("  Building...")
            sleep(2)
            x -= 1
        except Exception as e:
            print(e)
            break

    # Get the cluster endpoint and role ARN  (the cluster status must be "available" first)
    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']  # assumes no other roles exist and the first one returned is this one
    print("\n  DWH_ENDPOINT = ", DWH_ENDPOINT)
    print("  DWH_ROLE_ARN = ", DWH_ROLE_ARN)

    config.set("DWH","DWH_ENDPOINT", DWH_ENDPOINT)
    config.set("DWH","DWH_ROLE_ARN", DWH_ROLE_ARN)
    cfgfile = open("dwh.cfg", "w")
    config.write(cfgfile)
    cfgfile.close()

    return

def setup_db_connection(ec2, redshift, config):
    '''
    Opens a connecton to the database on the cluster

        Parameters:
            ec2: reference to the resource object
            redshift: reference to the client object
            config: reference to the configuration file

        Returns:
            conn: database connection 
            cur: database cursor
    '''

    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    print ("\nConnecting to cluster database...")
    # Open an incoming  TCP port to access the cluster ednpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]  # assumes no other security groups exist and first one in list is correct
        print(" ", defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        # print(e)
        pass

    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    print ("  Connected. Postgres connection string = ", conn_string)

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    return conn, cur

def load_tables(conn, cur):
    '''
    Creates and loads all of the database tables

        Parameters:
            conn: database connection 
            cur: database cursor

        Returns:
            nothing
    '''

    def drop_tables(cur, conn):
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()

    def create_tables(cur, conn):
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()

    def load_staging_tables(cur, conn):
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()

    def insert_tables(cur, conn):
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()

    def check_tables(cur, conn):
        for query in validation_queries:
            print (query)
            cur.execute(query)
            print(cur.fetchone())
            conn.commit()

    t = time()
    print ("\nCreating tables...")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print ("  Tables created.  (took {:0.0f} secs)".format(time() - t))

    t = time()
    print ("Copying S3 data to staging tables (will take several minutes)...")
    load_staging_tables(cur, conn)
    print ("  Data copy done.  (took {:0.0f} secs)".format(time() - t))

    t = time()
    print ("Loading analytical tables...")
    insert_tables(cur, conn)
    print ("  Analytical tables done.  (took {:0.0f} secs)".format(time() - t))

    print ("\nTable counts are as follows...")
    check_tables(cur, conn)

    return

def teardown(redshift, iam, config):
    '''
    Deletes the cluster and role so that nothing is left

        Parameters:
            redshift: reference to the client object
            iam: reference to the resource object
            config: reference to the configuration file

        Returns:
            nothing
    '''

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH","DWH_IAM_ROLE_NAME")

    res = redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print ("\nStart removal of cluster and roles...")
    print ("  Cluster Status: ", res['Cluster']['ClusterStatus'])

    x = 180
    while x > 0:
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if len(myClusterProps):
                if x % 10 == 0:
                    print ("  Waiting for cluster to finish deleting...")
            # if myClusterProps['ClusterStatus'] == 'available':
            #     print ("Redshift cluster has been created.")
            #     break
            sleep(2)
            x -= 1
        except Exception as e:
            print ("  Cluster deletion complete.")
            # print(e)
            break

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    res = iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    if res['ResponseMetadata']['HTTPStatusCode'] == 200:
        print ("  IAM role deleted")
    else:
        print ("Something went wrong")

    print ("\n  Clean up complete.")

    return

def main():
    '''
    Main function that handles the cluster creation, data loading and final cleanup

        Parameters:
            nothing

        Returns:
            nothing
    '''

    t = time()
    print("Starting Udacity Data Engineering Warehouse Project #3, aka IaaC awesomeness...")

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    ec2, iam, redshift = create_clients(config)

    roleArn = setup_iam_role(iam, config)

    setup_redshift_cluster(redshift, config, roleArn)

    conn, cur = setup_db_connection(ec2, redshift, config)

    load_tables(conn, cur)

    conn.close()

    teardown(redshift, iam, config)

    print ("\nTotal time to build cluster, extract/load/transform, and cleanup = {:0.1f} minutes".format( (time()-t) / 60 ))
    print ("\nThank you for shopping on Udacity.com")

    return

if __name__ == "__main__":
    main()