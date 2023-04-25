import mongoose, { ConnectOptions } from 'mongoose';

const mongoURI = process.env.MONGO_URI || 'mongodb://localhost:27017/library';

const connectDB = async (): Promise<void> => {
    try {
        await mongoose.connect(mongoURI);
        console.log('MongoDB connected...');
    } catch (err) {
        // @ts-ignore
        console.log(err.message);
        process.exit(1);
    }
};

export default connectDB;
