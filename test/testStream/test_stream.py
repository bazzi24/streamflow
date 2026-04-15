from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.from_collection([1,2,3,4,5]).map(lambda x: x * 10).print()
env.execute("Test PyFlink stream")

