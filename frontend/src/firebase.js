// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyBfINND2dF-OSsEmHqXemXo9euluh8Aoi4",
  authDomain: "lively-paratext-487716-r8.firebaseapp.com",
  projectId: "lively-paratext-487716-r8",
  storageBucket: "lively-paratext-487716-r8.firebasestorage.app",
  messagingSenderId: "1022135430610",
  appId: "1:1022135430610:web:a5cc7da0bc00efaac46c0b"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const db = getFirestore(app);
export default app;
