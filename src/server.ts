import dotenv from 'dotenv';
import express, { Application } from 'express';
import mongoose, {ConnectOptions} from 'mongoose';
import bodyParser from 'body-parser';
import cors from 'cors';
import bookRoutes from './routes/bookRoutes';

dotenv.config();

// Set up the Express app
const app: Application = express();

// Set up middleware
app.use(bodyParser.json());
app.use(cors());

// Set up routes
app.use('/books', bookRoutes);

// Set up the MongoDB connection
const mongoURI = process.env.MONGO_URI || 'mongodb://localhost:27017/library';

mongoose
    .connect(mongoURI)
    .then(() => console.log('MongoDB connected...'))
    .catch((err) => console.log(err));

// Set up the server
const PORT = process.env.PORT || 3000;
// app.listen(PORT, () => console.log(`Server started on port ${PORT}`));
export default app;
