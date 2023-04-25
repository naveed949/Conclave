import mongoose, { Document, Schema } from 'mongoose';

export interface IBook extends Document {
    title: string;
    author: string;
    publisher: string;
    isbn: string;
    copies: number;
    borrowedBy: string | null;
    borrowedDate: Date | null;
    dueDate: Date | null;
}

const BookSchema: Schema = new Schema({
    title: { type: String, required: true },
    author: { type: String, required: true },
    publisher: { type: String, required: true },
    isbn: { type: String, required: true, unique: true },
    copies: { type: Number, required: true },
    borrowedBy: { type: String, default: null },
    borrowedDate: { type: Date, default: null },
    dueDate: { type: Date, default: null },
});

export default mongoose.model<IBook>('Book', BookSchema);
